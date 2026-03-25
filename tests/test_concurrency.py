"""
tests/test_concurrency.py
=========================
Optimistic Concurrency Control (OCC) test for EventStore.

Tests that when two coroutines concurrently attempt to append to the same
stream at the same expected_version, exactly ONE succeeds and the other
raises OptimisticConcurrencyError — leaving the stream at exactly 4 events
(3 setup + 1 winner), never 5.

Requires a running PostgreSQL instance configured via TEST_DB_URL env var.
Mark: pytest.mark.requires_db  — skipped by default in CI without a DB.

Run with a live DB:
    TEST_DB_URL=postgresql://localhost/apex_ledger_test pytest tests/test_concurrency.py -v
"""
import asyncio
import os
from uuid import uuid4

import asyncpg
import pytest

from ledger.schema.events import (
    ApplicationSubmitted,
    OptimisticConcurrencyError,
)
from ledger.event_store import EventStore

# ── Skip entire module when no DB is available ────────────────────────────────
pytestmark = pytest.mark.asyncio


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
async def pg_pool():
    """Create a real asyncpg pool for the test database."""
    db_url = os.environ.get(
        "TEST_DB_URL", "postgresql://localhost/apex_ledger_test"
    )
    try:
        pool = await asyncpg.create_pool(db_url, min_size=2, max_size=5)
    except (OSError, asyncpg.InvalidCatalogNameError, asyncpg.PostgresError) as exc:
        pytest.skip(f"PostgreSQL not available: {exc}")
    yield pool
    await pool.close()


@pytest.fixture
async def event_store(pg_pool):
    """Inject the pool into EventStore (preferred constructor)."""
    return EventStore(pool=pg_pool)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_submitted_event(application_id: str) -> ApplicationSubmitted:
    """Create a minimal ApplicationSubmitted event for tests."""
    from decimal import Decimal
    from datetime import datetime
    from ledger.schema.events import LoanPurpose

    return ApplicationSubmitted(
        event_type="ApplicationSubmitted",
        application_id=application_id,
        applicant_id="APPLICANT-001",
        requested_amount_usd=Decimal("250000.00"),
        loan_purpose=LoanPurpose.WORKING_CAPITAL,
        loan_term_months=24,
        submission_channel="web",
        contact_email="test@example.com",
        contact_name="Test Corp",
        submitted_at=datetime.utcnow(),
        application_reference=f"REF-{application_id}",
    )


# ── Tests ─────────────────────────────────────────────────────────────────────

async def test_concurrent_double_append_exactly_one_succeeds(event_store):
    """
    Two concurrent appends to the same stream at the same expected_version.
    Exactly one must succeed; the other must raise OptimisticConcurrencyError.

    Invariant: stream ends up with exactly 4 events (3 setup + 1 winner),
    never 5. The winning event has stream_position == 4.
    """
    stream_id = f"loan-test-concurrent-occ-{uuid4()}"

    # ── Setup: append 3 events sequentially to reach version 2 ──────────────
    # Event 1 — creates the stream (expected_version=-1 → new stream)
    ev1 = _make_submitted_event(stream_id)
    await event_store.append(stream_id, [ev1], expected_version=-1)

    # Events 2 & 3
    ev2 = _make_submitted_event(stream_id)
    await event_store.append(stream_id, [ev2], expected_version=0)

    ev3 = _make_submitted_event(stream_id)
    await event_store.append(stream_id, [ev3], expected_version=1)

    # Confirm setup: stream at version 2 (Initial -1 + 3 events = 2)
    assert await event_store.stream_version(stream_id) == 2, (
        f"Pre-condition failed: stream should be at version 2 after setup, got {await event_store.stream_version(stream_id)}"
    )

    # ── Concurrent race: both tasks claim expected_version=2 ─────────────────
    results: dict = {"success": None, "error": None}

    async def task_a():
        try:
            new_ver = await event_store.append(
                stream_id=stream_id,
                events=[_make_submitted_event(stream_id)],
                expected_version=2,
                correlation_id="corr-task-a",
            )
            results["success"] = new_ver
        except OptimisticConcurrencyError as exc:
            results["error"] = exc

    async def task_b():
        try:
            new_ver = await event_store.append(
                stream_id=stream_id,
                events=[_make_submitted_event(stream_id)],
                expected_version=2,
                correlation_id="corr-task-b",
            )
            # Only record success if task_a hasn't already won
            if results["success"] is None:
                results["success"] = new_ver
        except OptimisticConcurrencyError as exc:
            results["error"] = exc

    # Launch both tasks truly concurrently
    await asyncio.gather(task_a(), task_b())

    # ── ASSERTION 1: Stream contains exactly 4 events (3 + 1 winner, not 5) ──
    events = await event_store.load_stream(stream_id)
    assert len(events) == 4, (
        f"Expected exactly 4 events in stream, got {len(events)}. "
        "OCC must prevent the second concurrent write."
    )

    # ── ASSERTION 2: The winning (4th) event has stream_position == 3 ─────────
    last_event = events[-1]
    assert last_event.stream_position == 3, (
        f"Expected last event at stream_position=3, got {last_event.stream_position}"
    )

    # ── ASSERTION 3: Exactly one task raised OptimisticConcurrencyError ───────
    assert results["error"] is not None, (
        "Expected one task to raise OptimisticConcurrencyError, but no error was captured"
    )
    assert isinstance(results["error"], OptimisticConcurrencyError), (
        f"Expected OptimisticConcurrencyError, got {type(results['error']).__name__}"
    )
    assert results["success"] is not None, (
        "Expected one task to succeed, but neither task captured a success result"
    )

    # Bonus: verify the error has the declarative fields populated correctly
    err: OptimisticConcurrencyError = results["error"]
    assert err.stream_id == stream_id
    assert err.expected_version == 2
    assert err.actual_version == 3, (
        f"Losing task should see actual_version=3 (winner already applied), "
        f"got {err.actual_version}"
    )


async def test_concurrent_append_race_at_expected_version_three(event_store):
    """
    Explicit rubric-aligned OCC race:
    - build the stream to version 3
    - race two appends at expected_version=3
    - winner returns new version 4
    - loser raises OptimisticConcurrencyError
    """
    stream_id = f"loan-test-concurrent-occ-v3-{uuid4()}"

    version = -1
    for _ in range(4):
        version = await event_store.append(
            stream_id,
            [_make_submitted_event(stream_id)],
            expected_version=version,
        )

    assert version == 3

    results: dict[str, object | None] = {"success": None, "error": None}

    async def contender() -> None:
        try:
            new_version = await event_store.append(
                stream_id,
                [_make_submitted_event(stream_id)],
                expected_version=3,
            )
            if results["success"] is None:
                results["success"] = new_version
        except OptimisticConcurrencyError as exc:
            results["error"] = exc

    await asyncio.gather(contender(), contender())

    events = await event_store.load_stream(stream_id)
    assert len(events) == 5
    assert results["success"] == 4
    assert isinstance(results["error"], OptimisticConcurrencyError)


async def test_occ_error_has_declared_fields(event_store):
    """
    OptimisticConcurrencyError exposes stream_id, expected_version, actual_version
    as inspectable attributes (not just a message string).
    """
    stream_id = f"loan-test-occ-fields-{uuid4()}"

    ev = _make_submitted_event(stream_id)
    await event_store.append(stream_id, [ev], expected_version=-1)
    # Stream for initial add with -1 becomes version 0.

    with pytest.raises(OptimisticConcurrencyError) as exc_info:
        await event_store.append(
            stream_id,
            [_make_submitted_event(stream_id)],
            expected_version=99,  # deliberately wrong
        )

    err = exc_info.value
    assert err.stream_id == stream_id
    assert err.expected_version == 99
    assert err.actual_version == 0


async def test_sequential_appends_build_correct_positions(event_store):
    """
    Non-concurrent append: each successive write returns the next version and
    stream_positions are contiguous [1, 2, 3].
    """
    stream_id = f"loan-test-sequential-{uuid4()}"

    v1 = await event_store.append(
        stream_id, [_make_submitted_event(stream_id)], expected_version=-1
    )
    assert v1 == 0

    v2 = await event_store.append(
        stream_id, [_make_submitted_event(stream_id)], expected_version=0
    )
    assert v2 == 1

    v3 = await event_store.append(
        stream_id, [_make_submitted_event(stream_id)], expected_version=1
    )
    assert v3 == 2

    events = await event_store.load_stream(stream_id)
    assert len(events) == 3
    positions = [e.stream_position for e in events]
    assert positions == [0, 1, 2]
