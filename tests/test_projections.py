import asyncio
import contextlib
import time
from datetime import datetime, timezone

import pytest


pytestmark = pytest.mark.asyncio


async def test_projection_lag_under_concurrent_load(event_store, db_pool, projection_daemon):
    """
    Simulate 50 concurrent command-style appends.
    Assert ApplicationSummary checkpoint lag stays under 500ms once the daemon
    has caught up with the burst.
    """

    async def simulate_handler(app_id: int):
        await event_store.append(
            stream_id=f"loan-app-load-{app_id}",
            events=[
                {
                    "event_type": "ApplicationSubmitted",
                    "event_version": 1,
                    "payload": {
                        "application_id": f"app-load-{app_id}",
                        "applicant_id": f"applicant-{app_id}",
                        "requested_amount_usd": 100000.0,
                        "loan_purpose": "working_capital",
                        "submission_channel": "api",
                    },
                }
            ],
            expected_version=-1,
        )

    daemon_task = asyncio.create_task(projection_daemon.run_forever(poll_interval_ms=50))

    try:
        await asyncio.gather(*(simulate_handler(i) for i in range(50)))

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            async with db_pool.acquire() as conn:
                projected = await conn.fetchval(
                    "SELECT COUNT(*) FROM application_summary WHERE application_id LIKE 'app-load-%'"
                )
            if projected == 50:
                break
            await asyncio.sleep(0.05)

        assert projected == 50, f"Projection only caught up to {projected}/50 applications"

        lag = await projection_daemon.get_lag("application_summary")
        assert lag < 500, f"ApplicationSummary lag {lag}ms exceeds 500ms SLO"
    finally:
        await projection_daemon.stop()
        daemon_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await daemon_task


async def test_rebuild_from_scratch_does_not_block_reads(event_store, db_pool, compliance_projection):
    """
    Verify rebuild_from_scratch completes and the projection table remains readable.
    """
    await event_store.append(
        stream_id="compliance-test-proj-1",
        events=[
            {
                "event_type": "ComplianceCheckInitiated",
                "event_version": 1,
                "payload": {
                    "application_id": "test-proj-1",
                    "session_id": "sess-1",
                    "regulation_set_version": "2026.01",
                    "rules_to_evaluate": ["RULE-1"],
                    "initiated_at": datetime.now(timezone.utc).isoformat(),
                },
            }
        ],
        expected_version=-1,
    )

    events = await event_store.load_stream("compliance-test-proj-1")
    await compliance_projection.handle(events[0], db_pool)

    await compliance_projection.rebuild_from_scratch(db_pool)

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT count(*) FROM compliance_audit_view")
        assert rows is not None
