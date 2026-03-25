"""
ledger/projections/daemon.py
=============================
Background asyncio task that polls the event store and routes events to
registered projections via:

    daemon = ProjectionDaemon(store, pool, [proj_a, proj_b])
    asyncio.create_task(daemon.run_forever())
    ...
    await daemon.stop()

Each projection must expose:
    .name               str
    .subscribed_events  list[str]
    async .handle(event, pool)
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import asyncpg

from ledger.event_store import EventStore

logger = logging.getLogger(__name__)


class ProjectionDaemon:
    """
    Background asyncio task that polls events and routes to projections.

    - Tracks per-projection checkpoints in the `projection_checkpoints` table.
    - Starts from the lowest checkpoint across all projections so every
      projection always sees every event it subscribes to.
    - Replays a small overlap window on each poll so rows that commit slightly
      later with lower global_position values are not skipped under concurrency.
    - Fault-tolerant: failed handlers are retried up to `max_retries` times
      before the event is skipped (with an ERROR log).
    """

    def __init__(
        self,
        store: EventStore,
        pool: asyncpg.Pool,
        projections: list[Any],
        max_retries: int = 3,
        replay_overlap: int = 100,
    ) -> None:
        self._store = store
        self._pool = pool
        self._projections: dict[str, Any] = {p.name: p for p in projections}
        self._running = False
        self._max_retries = max_retries
        self._replay_overlap = replay_overlap
        self._retry_counts: dict[int, int] = {}

    async def run_forever(self, poll_interval_ms: int = 100) -> None:
        """Main loop - run as a background asyncio task."""
        self._running = True
        logger.info(
            "ProjectionDaemon started with projections: %s",
            list(self._projections),
        )
        while self._running:
            try:
                await self._process_batch()
            except Exception as exc:
                logger.error("Daemon batch error: %s", exc, exc_info=True)
            await asyncio.sleep(poll_interval_ms / 1000)

    async def stop(self) -> None:
        """Signal the daemon to stop after the current batch completes."""
        self._running = False
        logger.info("ProjectionDaemon stopping.")

    async def _process_batch(self, batch_size: int = 100) -> None:
        """
        1. Find the lowest checkpoint across all registered projections.
        2. Load events from a small overlap before that checkpoint.
        3. Route each event to every projection that subscribes to it.
        4. Update all checkpoints to the maximum visible global_position.
        """
        min_position: float | int = float("inf")
        async with self._pool.acquire() as conn:
            for name in self._projections:
                row = await conn.fetchrow(
                    "SELECT last_position FROM projection_checkpoints WHERE projection_name = $1",
                    name,
                )
                pos = int(row["last_position"]) if row else 0
                if pos < min_position:
                    min_position = pos

        if min_position == float("inf"):
            min_position = 0

        start_position = max(int(min_position) - self._replay_overlap, -1)

        events: list[Any] = []
        async for event in self._store.load_all(
            from_global_position=start_position,
            batch_size=batch_size,
        ):
            events.append(event)

        if not events:
            return

        for event in events:
            et = event["event_type"] if isinstance(event, dict) else event.event_type
            gp = event["global_position"] if isinstance(event, dict) else event.global_position

            for name, projection in self._projections.items():
                if et not in projection.subscribed_events:
                    continue
                try:
                    await projection.handle(event, self._pool)
                    self._retry_counts.pop(gp, None)
                except Exception as exc:
                    retries = self._retry_counts.get(gp, 0) + 1
                    self._retry_counts[gp] = retries
                    if retries >= self._max_retries:
                        logger.error(
                            "Skipping event global_position=%s for projection %r after %d retries: %s",
                            gp,
                            name,
                            self._max_retries,
                            exc,
                        )
                        del self._retry_counts[gp]
                    else:
                        logger.warning(
                            "Retry %d/%d for event global_position=%s in projection %r: %s",
                            retries,
                            self._max_retries,
                            gp,
                            name,
                            exc,
                        )

        last_event = events[-1]
        max_position = (
            last_event["global_position"] if isinstance(last_event, dict) else last_event.global_position
        )
        async with self._pool.acquire() as conn:
            for name in self._projections:
                await conn.execute(
                    """
                    INSERT INTO projection_checkpoints
                        (projection_name, last_position, updated_at)
                    VALUES ($1, $2, NOW())
                    ON CONFLICT (projection_name)
                    DO UPDATE SET last_position = $2, updated_at = NOW()
                    """,
                    name,
                    max_position,
                )

    async def get_lag(self, projection_name: str) -> float:
        """
        Return the processing lag in milliseconds for a specific projection.

        Lag is measured as the wall-clock seconds since the checkpoint was
        last updated. Returns ``float('inf')`` if no checkpoint exists.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT EXTRACT(EPOCH FROM (NOW() - pc.updated_at)) * 1000 AS lag_ms
                FROM projection_checkpoints pc
                WHERE pc.projection_name = $1
                """,
                projection_name,
            )
        return float(row["lag_ms"]) if row else float("inf")

    async def get_all_lags(self) -> dict[str, float]:
        """Return lag in milliseconds for every registered projection."""
        return {name: await self.get_lag(name) for name in self._projections}
