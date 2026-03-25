"""
tests/test_gas_town.py
======================
Tests for cold context reconstruction — ledger/integrity/gas_town.py
"""
from __future__ import annotations

import pytest
from ledger.event_store import InMemoryEventStore
from ledger.integrity.gas_town import reconstruct_agent_context


# ── Shared fixture: in-memory store (no DB required) ─────────────────────────

@pytest.fixture
def mem_store():
    return InMemoryEventStore()


# ── Helpers to build plain event dicts (InMemoryEventStore format) ────────────

def _make_event(event_type: str, payload: dict | None = None) -> dict:
    return {
        "event_type": event_type,
        "event_version": 1,
        "payload": payload or {},
    }


# ── Tests ─────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_crash_recovery_cold_reconstruction(mem_store):
    """
    1. Start agent session (append AgentSessionStarted)
    2. Append 4 more events (AgentNodeExecuted x3, CreditAnalysisCompleted)
       — total 5 events in the stream
    3. DO NOT keep any in-memory agent reference
    4. Call reconstruct_agent_context() cold
    5. Assert:
       - pending_work is non-empty
       - last_event_position == 4  (0-based: positions 0,1,2,3,4)
       - context_text is non-empty
       - session_health_status == "NEEDS_RECONCILIATION"
         (CreditAnalysisCompleted is a decision event with no completion event)
    """
    session_id = "test-session-crash"
    agent_id = "credit"
    stream_id = f"agent-{agent_id}-{session_id}"

    events_to_append = [
        _make_event("AgentSessionStarted", {"session_id": session_id, "agent_type": "credit_analysis"}),
        _make_event("AgentNodeExecuted", {"node_name": "validate_inputs", "session_id": session_id}),
        _make_event("AgentNodeExecuted", {"node_name": "load_external_data", "session_id": session_id}),
        _make_event("AgentNodeExecuted", {"node_name": "analyze_credit_risk", "session_id": session_id}),
        _make_event("CreditAnalysisCompleted", {"session_id": session_id, "application_id": "APP-001"}),
        # ↑ Decision event — no AgentSessionCompleted follows → NEEDS_RECONCILIATION
    ]

    version = -1
    for event in events_to_append:
        positions = await mem_store.append(stream_id, [event], expected_version=version)
        version = positions[-1]  # advance to last written position

    # Cold reconstruction — no in-memory state
    context = await reconstruct_agent_context(mem_store, agent_id, session_id)

    # ── Assertions ──────────────────────────────────────────────────────────
    assert context.events_total == 5, f"Expected 5 events, got {context.events_total}"

    # InMemoryEventStore assigns stream_position starting at 0
    assert context.last_event_position == 4, (
        f"Expected last_event_position=4, got {context.last_event_position}"
    )

    assert context.context_text != "", "context_text must not be empty"

    assert context.session_health_status == "NEEDS_RECONCILIATION", (
        f"Expected NEEDS_RECONCILIATION, got {context.session_health_status!r}"
    )

    assert len(context.pending_work) > 0, "pending_work must be non-empty"

    # CreditAnalysisCompleted is the last completed action (it's a DECISION_EVENT)
    assert context.last_completed_action == "Decision: CreditAnalysisCompleted"


@pytest.mark.asyncio
async def test_empty_stream_returns_failed(mem_store):
    """Reconstructing a stream that doesn't exist returns FAILED + Session not started."""
    context = await reconstruct_agent_context(mem_store, "credit", "nonexistent-session")

    assert context.session_health_status == "FAILED"
    assert context.last_event_position == 0
    assert "Session not started" in context.pending_work


@pytest.mark.asyncio
async def test_completed_session_is_healthy(mem_store):
    """A fully completed session (with AgentSessionCompleted) is HEALTHY."""
    session_id = "test-session-ok"
    agent_id = "fraud"
    stream_id = f"agent-{agent_id}-{session_id}"

    events = [
        _make_event("AgentSessionStarted", {"session_id": session_id}),
        _make_event("AgentNodeExecuted", {"node_name": "validate_inputs"}),
        _make_event("FraudScreeningCompleted", {"session_id": session_id}),
        _make_event("AgentSessionCompleted", {"session_id": session_id}),
    ]

    version = -1
    for ev in events:
        positions = await mem_store.append(stream_id, [ev], expected_version=version)
        version = positions[-1]

    context = await reconstruct_agent_context(mem_store, agent_id, session_id)

    assert context.session_health_status == "HEALTHY"
    assert context.events_total == 4


@pytest.mark.asyncio
async def test_recoverable_failure_is_needs_reconciliation(mem_store):
    """A recoverable AgentSessionFailed upgrades from FAILED to NEEDS_RECONCILIATION."""
    session_id = "test-session-recoverable"
    agent_id = "compliance"
    stream_id = f"agent-{agent_id}-{session_id}"

    events = [
        _make_event("AgentSessionStarted", {"session_id": session_id}),
        _make_event("AgentNodeExecuted", {"node_name": "validate_inputs"}),
        _make_event(
            "AgentSessionFailed",
            {
                "session_id": session_id,
                "recoverable": True,
                "last_successful_node": "validate_inputs",
                "error_message": "Timeout",
                "error_type": "TimeoutError",
            },
        ),
    ]

    version = -1
    for ev in events:
        positions = await mem_store.append(stream_id, [ev], expected_version=version)
        version = positions[-1]

    context = await reconstruct_agent_context(mem_store, agent_id, session_id)

    assert context.session_health_status == "NEEDS_RECONCILIATION"
    assert any("validate_inputs" in item for item in context.pending_work)
