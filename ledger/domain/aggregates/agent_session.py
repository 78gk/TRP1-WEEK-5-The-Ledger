from __future__ import annotations
from dataclasses import dataclass, field

from ledger.schema.events import DomainError, StoredEvent


def _candidate_stream_ids(agent_id: str, session_id: str) -> list[str]:
    mapped = {
        "credit-agent": "credit_analysis",
        "fraud-agent": "fraud_detection",
        "compliance-agent": "compliance",
        "decision-agent": "decision_orchestrator",
    }.get(agent_id)
    candidates = [f"agent-{agent_id}-{session_id}"]
    if mapped and mapped != agent_id:
        candidates.append(f"agent-{mapped}-{session_id}")
    return candidates


@dataclass
class AgentSessionAggregate:
    session_id: str
    agent_id: str
    _context_declared: bool = field(default=False)
    _model_version: str | None = field(default=None)
    _agent_type: str | None = field(default=None)
    _nodes_executed: int = field(default=0)
    version: int = 0

    @classmethod
    async def load(cls, event_store, agent_id: str, session_id: str) -> "AgentSessionAggregate":
        agg = cls(session_id=session_id, agent_id=agent_id)
        events = []
        for stream_id in _candidate_stream_ids(agent_id, session_id):
            events = await event_store.load_stream(stream_id)
            if events:
                break
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: StoredEvent) -> None:
        self.version = event.stream_position
        handler_name = f"_on_{event.event_type}"
        handler = getattr(self, handler_name, None)
        if handler:
            handler(event)

    def _on_AgentSessionStarted(self, event: StoredEvent) -> None:
        payload = event.payload if isinstance(event.payload, dict) else {}
        self._context_declared = True
        self._agent_type = payload.get("agent_type")
        self._model_version = payload.get("model_version")

    def _on_AgentContextLoaded(self, event: StoredEvent) -> None:
        self._context_declared = True
        payload = event.payload if isinstance(event.payload, dict) else {}
        self._model_version = payload.get("model_version", self._model_version)
        self._agent_type = payload.get("agent_type", self._agent_type)

    def _on_AgentNodeExecuted(self, event: StoredEvent) -> None:
        self._nodes_executed += 1

    def assert_context_loaded(self) -> None:
        if not self._context_declared:
            raise DomainError(
                f"Agent session {self.session_id} has not declared its context. "
                f"Cannot accept session events before AgentSessionStarted."
            )

    def assert_model_version_current(self, version: str) -> None:
        if self._model_version != version:
            raise DomainError(
                f"Model version mismatch in session {self.session_id}: "
                f"started with {self._model_version!r}, but attempted to use {version!r}"
            )
