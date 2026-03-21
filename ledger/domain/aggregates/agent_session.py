from __future__ import annotations
from dataclasses import dataclass, field

from ledger.schema.events import DomainError, StoredEvent


@dataclass
class AgentSessionAggregate:
    session_id: str
    agent_id: str
    _context_declared: bool = field(default=False)
    _model_version: str | None = field(default=None)
    _agent_type: str | None = field(default=None)
    _nodes_executed: int = field(default=0)
    version: int = 0  # Mirrors the stream's current_version

    @classmethod
    async def load(cls, event_store, agent_id: str, session_id: str) -> "AgentSessionAggregate":
        """Load and replay event stream to rebuild aggregate state."""
        agg = cls(session_id=session_id, agent_id=agent_id)
        # Assuming the agent session stream follows naming convention agent-{agent_id}-{session_id} or just session_id,
        # The canonical comments in events.py say: stream: "agent-{agent_type}-{session_id}"
        # We will assume caller provides the correct agent_id/agent_type string.
        # But looking at handlers prompt, it passes: agent_id, session_id. So we use f"agent-{agent_id}-{session_id}"
        stream_id = f"agent-{agent_id}-{session_id}"
        events = await event_store.load_stream(stream_id)
        
        for event in events:
            agg._apply(event)
            
        return agg

    def _apply(self, event: StoredEvent) -> None:
        """
        Dynamically dispatches to _on_<EventType> based on the event's type.
        """
        self.version = event.stream_position
        
        handler_name = f"_on_{event.event_type}"
        handler = getattr(self, handler_name, None)
        
        if handler:
            handler(event)

    # ─── EVENT HANDLERS ───────────────────────────────────────────────────────
    
    def _on_AgentSessionStarted(self, event: StoredEvent) -> None:
        payload = event.payload
        if isinstance(payload, dict):
            self._agent_type = payload.get("agent_type")
        else:
            self._agent_type = getattr(payload, "agent_type", None)

    def _on_AgentContextLoaded(self, event: StoredEvent) -> None:
        """Supports the explicit context-loading event name used in some scenarios."""
        self._context_declared = True

        payload = event.payload
        if isinstance(payload, dict):
            self._model_version = payload.get("model_version")
            self._agent_type = payload.get("agent_type", self._agent_type)
        else:
            self._model_version = getattr(payload, "model_version", None)
            self._agent_type = getattr(payload, "agent_type", self._agent_type)

    def _on_AgentNodeExecuted(self, event: StoredEvent) -> None:
        self._nodes_executed += 1

    # ─── GUARDS / BUSINESS RULES ──────────────────────────────────────────────
    
    def assert_context_loaded(self) -> None:
        """
        Validates that AgentSessionStarted has been applied before allowing outputs.
        """
        if not self._context_declared:
            raise DomainError(
                f"Agent session {self.session_id} has not declared its context. "
                f"Cannot accept session events before AgentSessionStarted."
            )

    def assert_model_version_current(self, version: str) -> None:
        """
        Validates the current executing model version matches the version declared
        at session start.
        """
        if self._model_version != version:
            raise DomainError(
                f"Model version mismatch in session {self.session_id}: "
                f"started with {self._model_version!r}, but attempted to use {version!r}"
            )
