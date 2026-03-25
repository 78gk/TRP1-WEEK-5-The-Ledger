"""
ledger/event_store.py — PostgreSQL-backed EventStore
=====================================================
COMPLETION CHECKLIST (implement in order):
  [ ] Phase 1, Day 1: append() + stream_version()
  [ ] Phase 1, Day 1: load_stream()
  [ ] Phase 1, Day 2: load_all()  (needed for projection daemon)
  [ ] Phase 1, Day 2: get_event() (needed for causation chain)
  [ ] Phase 4:        UpcasterRegistry.upcast() integration in load_stream/load_all
"""
from __future__ import annotations
import json
from datetime import datetime
from typing import AsyncGenerator, Any
from uuid import UUID
import asyncpg

# Canonical exception / model types — always import from here, never re-define
from ledger.schema.events import (
    OptimisticConcurrencyError,  # noqa: F401  (re-exported for backwards compat)
    DomainError,                 # noqa: F401
    StoredEvent,                 # noqa: F401
    StreamMetadata,
)
from ledger.upcasting import UpcasterRegistry as CanonicalUpcasterRegistry
from ledger.upcasting import registry as default_upcaster_registry


# OptimisticConcurrencyError is now defined in ledger.schema.events and imported above.
# It is re-raised from that module everywhere (InMemoryEventStore.append, EventStore.append).


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _extract_aggregate_type(stream_id: str) -> str:
    """Derive aggregate type from stream_id prefix (e.g. 'loan-APP-001' → 'loan')."""
    return stream_id.split("-")[0]


def _row_to_stored_event(row) -> StoredEvent:
    """Convert a raw asyncpg Record (or dict) to a typed StoredEvent."""
    d = dict(row)
    # asyncpg may return JSONB columns as dicts already; normalise to dict
    if isinstance(d.get("payload"), str):
        d["payload"] = json.loads(d["payload"])
    if isinstance(d.get("metadata"), str):
        d["metadata"] = json.loads(d["metadata"])
    # Ensure UUID type
    if isinstance(d.get("event_id"), str):
        d["event_id"] = UUID(d["event_id"])
    return StoredEvent(**d)


# ─────────────────────────────────────────────────────────────────────────────
# EVENTSTORE — PostgreSQL-backed, asyncpg
# ─────────────────────────────────────────────────────────────────────────────

class EventStore:
    """
    Append-only PostgreSQL event store.

    Two construction styles are supported for backwards-compatibility:
      1. Pool-based (preferred):  EventStore(pool=pool)
      2. URL-based (legacy):      EventStore(db_url); await store.connect()

    Public API (all async):
      stream_version()      → int
      append()              → int  (new version)
      load_stream()         → list[StoredEvent]
      load_all()            → AsyncIterator[StoredEvent]
      archive_stream()      → None
      get_stream_metadata() → StreamMetadata
    """

    def __init__(
        self,
        db_url: str | None = None,
        upcaster_registry=None,
        *,
        pool: asyncpg.Pool | None = None,
    ):
        # Accept either (pool=pool) or (db_url) for backwards-compat.
        self._pool: asyncpg.Pool | None = pool
        self._db_url: str | None = db_url
        self.upcasters = upcaster_registry or default_upcaster_registry

    # ── Lifecycle (only needed for the URL-based path) ───────────────────────

    async def connect(self) -> None:
        """Create a connection pool from db_url. Skip if pool was injected."""
        if self._pool is None:
            if self._db_url is None:
                raise RuntimeError("EventStore requires either pool= or db_url=")
            self._pool = await asyncpg.create_pool(
                self._db_url, min_size=2, max_size=10
            )

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    # ── Internal helpers ─────────────────────────────────────────────────────

    @property
    def _p(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("EventStore not connected. Call await store.connect() first.")
        return self._pool

    def _to_stored(self, row) -> StoredEvent:
        return _row_to_stored_event(row)

    # ── Public API ───────────────────────────────────────────────────────────

    async def stream_version(self, stream_id: str) -> int:
        """Return current version of stream. Returns -1 if stream does not exist."""
        async with self._p.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
            return int(row["current_version"]) if row else -1

    async def append(
        self,
        stream_id: str,
        events: "list[BaseEvent]",
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> int:
        """
        Atomically append events to a stream with optimistic concurrency control.

        OCC is enforced at the database level with SELECT ... FOR UPDATE,
        which serialises concurrent appenders on the same stream row and lets
        PostgreSQL guarantee exactly-once ordering.

        Both the *events* table and the *outbox* table are written in the same
        transaction so downstream consumers see events only after they are
        durably stored.

        Parameters
        ----------
        stream_id:        Target stream (e.g. "loan-APP-0001").
        events:           List of BaseEvent (or any model with .event_type /
                          .event_version / .model_dump(mode='json')).
        expected_version: The version the caller believes the stream is at
                          right now. Pass -1 when creating a new stream.
        correlation_id:   Propagated trace ID, stored in metadata JSONB.
        causation_id:     ID of the event that caused this write.

        Returns
        -------
        int — the new current_version of the stream after the append.

        Raises
        ------
        OptimisticConcurrencyError — if actual version ≠ expected_version.
        """
        from uuid import uuid4

        metadata = {
            "correlation_id": correlation_id,
            "causation_id": causation_id,
        }

        async with self._p.acquire() as conn:
            async with conn.transaction():

                # ── Step 1: Lock stream row and read current version ─────────
                row = await conn.fetchrow(
                    "SELECT current_version FROM event_streams "
                    "WHERE stream_id = $1 FOR UPDATE",
                    stream_id,
                )

                if row is None:
                    # Stream does not exist yet.
                    if expected_version != -1:
                        raise OptimisticConcurrencyError(
                            stream_id=stream_id,
                            expected_version=expected_version,
                            actual_version=-1,
                        )
                    # ── Step 1a: Create the stream record ──────────────────
                    await conn.execute(
                        "INSERT INTO event_streams "
                        "(stream_id, aggregate_type, current_version) "
                        "VALUES ($1, $2, -1)",
                        stream_id,
                        _extract_aggregate_type(stream_id),
                    )
                    actual_version = -1
                else:
                    actual_version = int(row["current_version"])
                    if actual_version != expected_version:
                        raise OptimisticConcurrencyError(
                            stream_id=stream_id,
                            expected_version=expected_version,
                            actual_version=actual_version,
                        )

                # ── Step 2: Insert each event ────────────────────────────────
                new_version = actual_version
                for i, event in enumerate(events):
                    position = actual_version + i + 1
                    new_version = position
                    event_id = uuid4()

                    # Payload: exclude envelope fields so only business data stored.
                    payload_dict: dict[str, Any]
                    if hasattr(event, "to_payload"):
                        payload_dict = event.to_payload()
                        event_type = event.event_type
                        event_version = event.event_version
                    elif hasattr(event, "model_dump"):
                        dumped = event.model_dump(mode="json")
                        event_type = dumped["event_type"]
                        event_version = dumped.get("event_version", 1)
                        payload_dict = {
                            k: v for k, v in dumped.items()
                            if k not in {"event_type", "event_version"}
                        }
                    else:
                        event_type = event["event_type"]
                        event_version = event.get("event_version", 1)
                        payload_dict = dict(event.get("payload", {}))

                    await conn.execute(
                        """
                        INSERT INTO events
                            (event_id, stream_id, stream_position,
                             event_type, event_version, payload, metadata)
                        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb)
                        """,
                        event_id,
                        stream_id,
                        position,
                        event_type,
                        event_version,
                        json.dumps(payload_dict),
                        json.dumps(metadata),
                    )

                    # ── Step 3: Outbox (same transaction) ───────────────────
                    await conn.execute(
                        """
                        INSERT INTO outbox (event_id, destination, payload)
                        VALUES ($1, $2, $3::jsonb)
                        """,
                        event_id,
                        "default",
                        json.dumps(payload_dict),
                    )

                # ── Step 4: Advance stream version ───────────────────────────
                await conn.execute(
                    "UPDATE event_streams SET current_version = $1 "
                    "WHERE stream_id = $2",
                    new_version,
                    stream_id,
                )

                return new_version

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> "list[StoredEvent]":
        """
        Load all events for *stream_id* in stream_position order.

        Parameters
        ----------
        from_position: Inclusive lower bound on stream_position (default 0).
        to_position:   Inclusive upper bound on stream_position (default: no limit).

        Returns
        -------
        list[StoredEvent] — may be empty if stream not found or range has no events.
        """
        q = (
            "SELECT event_id, stream_id, stream_position, global_position, "
            "event_type, event_version, payload, metadata, recorded_at "
            "FROM events "
            "WHERE stream_id = $1 AND stream_position >= $2"
        )
        params: list = [stream_id, from_position]

        if to_position is not None:
            q += " AND stream_position <= $3"
            params.append(to_position)

        q += " ORDER BY stream_position ASC"

        async with self._p.acquire() as conn:
            rows = await conn.fetch(q, *params)

        result = []
        for row in rows:
            d = dict(row)
            event = self._to_stored(d)
            if self.upcasters:
                event = self.upcasters.upcast(event)
            result.append(event)

        return result

    async def load_all(
        self,
        from_global_position: int = 0,
        event_types: "list[str] | None" = None,
        batch_size: int = 500,
    ) -> "AsyncGenerator[StoredEvent, None]":
        """
        Async generator that yields all events in global_position order.

        Streams in batches of *batch_size* rows to avoid loading the whole
        table into memory at once.  Used by the ProjectionDaemon.

        Parameters
        ----------
        from_global_position: Start after this global position (exclusive).
        event_types:          Optional allow-list of event_type strings.
        batch_size:           Rows per database round-trip (default 500).

        Yields
        ------
        StoredEvent — one per database row, in global_position order.
        """
        position = from_global_position

        while True:
            q = (
                "SELECT event_id, stream_id, stream_position, global_position, "
                "event_type, event_version, payload, metadata, recorded_at "
                "FROM events WHERE global_position > $1"
            )
            params: list = [position]

            if event_types:
                q += " AND event_type = ANY($2)"
                params.append(event_types)

            q += f" ORDER BY global_position ASC LIMIT ${len(params) + 1}"
            params.append(batch_size)

            async with self._p.acquire() as conn:
                rows = await conn.fetch(q, *params)

            if not rows:
                break

            for row in rows:
                d = dict(row)
                event = self._to_stored(d)
                if self.upcasters:
                    event = self.upcasters.upcast(event)
                yield event

            position = int(rows[-1]["global_position"])

            if len(rows) < batch_size:
                # Last partial page — no more data.
                break

    async def archive_stream(self, stream_id: str) -> None:
        """
        Mark a stream as archived by setting archived_at to the current time.

        Archived streams are excluded from live queries by convention but are
        never physically deleted (immutability guarantee).
        """
        async with self._p.acquire() as conn:
            await conn.execute(
                "UPDATE event_streams SET archived_at = NOW() "
                "WHERE stream_id = $1",
                stream_id,
            )

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata:
        """
        Return a StreamMetadata object for the given stream.

        Raises
        ------
        KeyError — if the stream does not exist.
        """
        async with self._p.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT stream_id, aggregate_type, current_version, "
                "created_at, archived_at "
                "FROM event_streams WHERE stream_id = $1",
                stream_id,
            )

        if row is None:
            raise KeyError(f"Stream not found: {stream_id!r}")

        return StreamMetadata(**dict(row))


# ─────────────────────────────────────────────────────────────────────────────
# UPCASTER REGISTRY — Phase 4
# ─────────────────────────────────────────────────────────────────────────────

class UpcasterRegistry(CanonicalUpcasterRegistry):
    """
    Transforms old event versions to current versions on load.
    Upcasters are PURE functions — they never write to the database.

    REGISTER AN UPCASTER:
        registry = UpcasterRegistry()

        @registry.upcaster("CreditAnalysisCompleted", from_version=1, to_version=2)
        def upcast_credit_v1_v2(payload: dict) -> dict:
            # v2 adds model_versions dict
            payload.setdefault("model_versions", {})
            return payload

    REQUIRED FOR PHASE 4:
        - CreditAnalysisCompleted  v1 → v2  (adds model_versions: dict)
        - DecisionGenerated        v1 → v2  (adds model_versions: dict)

    IMMUTABILITY TEST (required artifact):
        registry.assert_upcaster_does_not_write_to_db(store, event)
        # Loads the event, upcasts it, re-loads it, confirms DB row unchanged.
    """

    def __init__(self):
        super().__init__()

    def upcaster(self, event_type: str, from_version: int, to_version: int):
        return self.register(event_type, from_version)

    def upcast(self, event: dict) -> dict:
        return super().upcast(event)


# ─────────────────────────────────────────────────────────────────────────────
# IN-MEMORY EVENT STORE — for Phase 1 tests only
# Identical interface to EventStore. Drop-in for tests; never use in production.
# ─────────────────────────────────────────────────────────────────────────────

import asyncio as _asyncio
from collections import defaultdict as _defaultdict
from datetime import datetime as _datetime
from datetime import timezone as _timezone
from uuid import uuid4 as _uuid4

class InMemoryEventStore:
    """
    Thread-safe (asyncio-safe) in-memory event store.
    Used exclusively in Phase 1 tests and conftest fixtures.
    Same interface as EventStore — swap one for the other with no code changes.
    """

    def __init__(self):
        # stream_id -> list of event dicts
        self._streams: dict[str, list[dict]] = _defaultdict(list)
        # stream_id -> current version (position of last event, -1 if empty)
        self._versions: dict[str, int] = {}
        # global append log (ordered by insertion)
        self._global: list[dict] = []
        # projection checkpoints
        self._checkpoints: dict[str, int] = {}
        # asyncio lock per stream for OCC
        self._locks: dict[str, _asyncio.Lock] = _defaultdict(_asyncio.Lock)

    async def stream_version(self, stream_id: str) -> int:
        return self._versions.get(stream_id, -1)

    async def append(
        self,
        stream_id: str,
        events: list[dict],
        expected_version: int,
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:
        async with self._locks[stream_id]:
            current = self._versions.get(stream_id, -1)
            if current != expected_version:
                raise OptimisticConcurrencyError(stream_id, expected_version, current)

            positions = []
            meta = {**(metadata or {})}
            if causation_id:
                meta["causation_id"] = causation_id

            for i, event in enumerate(events):
                pos = current + 1 + i
                stored = {
                    "event_id": str(_uuid4()),
                    "stream_id": stream_id,
                    "stream_position": pos,
                    "global_position": len(self._global),
                    "event_type": event["event_type"],
                    "event_version": event.get("event_version", 1),
                    "payload": dict(event.get("payload", {})),
                    "metadata": meta,
                    "recorded_at": _datetime.now(_timezone.utc).isoformat(),
                }
                self._streams[stream_id].append(stored)
                self._global.append(stored)
                positions.append(pos)

            self._versions[stream_id] = current + len(events)
            return positions

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[dict]:
        events = [
            e for e in self._streams.get(stream_id, [])
            if e["stream_position"] >= from_position
            and (to_position is None or e["stream_position"] <= to_position)
        ]
        return sorted(events, key=lambda e: e["stream_position"])

    async def load_all(self, from_position: int = 0, batch_size: int = 500):
        for e in self._global:
            if e["global_position"] >= from_position:
                yield e

    async def get_event(self, event_id: str) -> dict | None:
        for e in self._global:
            if e["event_id"] == event_id:
                return e
        return None

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        self._checkpoints[projection_name] = position

    async def load_checkpoint(self, projection_name: str) -> int:
        return self._checkpoints.get(projection_name, 0)
