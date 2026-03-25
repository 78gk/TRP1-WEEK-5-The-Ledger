"""
ledger/integrity/gas_town.py
============================
Cold context reconstruction for agent sessions — "Gas Town" (the recovery town).

When an agent crashes or a session is interrupted, this module reconstructs
the agent's full context purely from its event stream, with zero in-memory state.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from ledger.event_store import EventStore


@dataclass
class AgentContext:
    """Typed return value for agent context reconstruction."""
    context_text: str              # Token-efficient prose summary
    last_event_position: int       # Stream position of last event
    pending_work: list[str]        # List of pending work item descriptions
    session_health_status: str     # "HEALTHY" | "NEEDS_RECONCILIATION" | "FAILED"
    events_total: int = 0
    last_completed_action: str | None = None
    verbatim_recent_events: list[dict] = field(default_factory=list)


# Event types that represent decisions (need a subsequent completion event)
DECISION_EVENTS = {
    "CreditAnalysisCompleted",
    "FraudScreeningCompleted",
    "DecisionGenerated",
}

# Event types that represent a successful end-of-session
COMPLETION_EVENTS = {
    "AgentSessionCompleted",
    "AgentOutputWritten",
}

# Events indicating error or terminal-pending state
ERROR_PENDING_EVENTS = {
    "AgentSessionFailed",
    "AgentInputValidationFailed",
}


def _event_type(event) -> str:
    """Safely extract event_type from either a dict or a StoredEvent-like object."""
    if isinstance(event, dict):
        return event["event_type"]
    return event.event_type


def _event_position(event) -> int:
    """Safely extract stream_position from either a dict or a StoredEvent-like object."""
    if isinstance(event, dict):
        return event["stream_position"]
    return event.stream_position


def _event_payload(event) -> dict:
    """Safely extract payload from either a dict or a StoredEvent-like object."""
    if isinstance(event, dict):
        return event.get("payload", {})
    return event.payload


async def reconstruct_agent_context(
    store: EventStore,
    agent_id: str,
    session_id: str,
    token_budget: int = 8000,
) -> AgentContext:
    """
    Reconstruct an agent's context from its event stream COLD
    (without any in-memory agent state).

    Steps:
    1. Load full AgentSession stream for agent_id + session_id
    2. Identify: last completed action, pending work items, current state
    3. Summarise old events into token-efficient prose
    4. Preserve verbatim: last 3 events and any ERROR/PENDING state events
    5. Detect NEEDS_RECONCILIATION: if last event was a decision without
       a corresponding completion event
    6. Return typed AgentContext
    """
    # AgentSession streams follow the format: agent-{agent_type}-{session_id}
    stream_id = f"agent-{agent_id}-{session_id}"

    events = await store.load_stream(stream_id)

    if not events:
        return AgentContext(
            context_text="No events found for this session.",
            last_event_position=0,
            pending_work=["Session not started"],
            session_health_status="FAILED",
        )

    # ── Pass 1: classify events ──────────────────────────────────────────────
    last_completed = None
    completed_nodes: set[str] = set()
    pending_work: list[str] = []
    has_completion = False
    has_decision = False
    error_events = []

    for event in events:
        et = _event_type(event)
        payload = _event_payload(event)

        if et == "AgentNodeExecuted":
            node_name = payload.get("node_name", "unknown")
            completed_nodes.add(node_name)
            last_completed = f"Node: {node_name}"

        if et in DECISION_EVENTS:
            has_decision = True
            last_completed = f"Decision: {et}"

        if et in COMPLETION_EVENTS:
            has_completion = True

        if et in ERROR_PENDING_EVENTS:
            error_events.append(event)

    # ── Session health determination ─────────────────────────────────────────
    last_event = events[-1]
    last_et = _event_type(last_event)
    last_payload = _event_payload(last_event)
    session_health = "HEALTHY"

    if last_et in ERROR_PENDING_EVENTS:
        session_health = "FAILED"
    elif has_decision and not has_completion:
        # A decision event was written but no AgentSessionCompleted/AgentOutputWritten
        session_health = "NEEDS_RECONCILIATION"
        pending_work.append(
            f"Decision event {last_et} found but no completion event — "
            "reconciliation required"
        )

    # A recoverable failure overrides FAILED → NEEDS_RECONCILIATION
    if last_et == "AgentSessionFailed":
        recoverable = last_payload.get("recoverable", False)
        if recoverable:
            session_health = "NEEDS_RECONCILIATION"
            last_node = last_payload.get("last_successful_node", "unknown")
            pending_work.append(f"Resume from after node: {last_node}")

    # ── Identify nodes that were expected but never executed ─────────────────
    expected_nodes = [
        "validate_inputs",
        "open_aggregate_record",
        "load_external_data",
        "write_output",
    ]
    for node in expected_nodes:
        if node not in completed_nodes:
            pending_work.append(f"Node not yet executed: {node}")

    # ── Build verbatim event dicts ───────────────────────────────────────────
    tail = events[-3:] if len(events) >= 3 else events
    verbatim_dicts = [
        {
            "event_type": _event_type(e),
            "position": _event_position(e),
            "payload": _event_payload(e),
        }
        for e in tail
    ]

    # Also preserve error events that are not already in the tail
    tail_set = set(id(e) for e in tail)
    for ee in error_events:
        if id(ee) not in tail_set:
            verbatim_dicts.append(
                {
                    "event_type": _event_type(ee),
                    "position": _event_position(ee),
                    "payload": _event_payload(ee),
                }
            )

    # ── Summarise older events into prose ────────────────────────────────────
    error_set = set(id(e) for e in error_events)
    older_events = events[:-3] if len(events) > 3 else []
    summary_parts = [
        f"[Position {_event_position(e)}] {_event_type(e)}"
        for e in older_events
        if id(e) not in error_set
    ]

    summary_text = (
        "Session summary: " + " → ".join(summary_parts)
        if summary_parts
        else ""
    )
    recent_text = f"Recent events (verbatim): {verbatim_dicts}"

    context_text = f"{summary_text}\n\n{recent_text}"

    # Truncate summary if total would exceed token budget (≈4 chars/token)
    max_chars = token_budget * 4
    if len(context_text) > max_chars:
        available = max_chars - len(recent_text) - 20
        context_text = summary_text[:available] + f"...\n\n{recent_text}"

    return AgentContext(
        context_text=context_text,
        last_event_position=_event_position(events[-1]),
        pending_work=pending_work if pending_work else ["No pending work identified"],
        session_health_status=session_health,
        events_total=len(events),
        last_completed_action=last_completed,
        verbatim_recent_events=verbatim_dicts,
    )
