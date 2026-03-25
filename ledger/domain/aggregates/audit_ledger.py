"""
ledger/domain/aggregates/audit_ledger.py
=========================================
AuditLedgerAggregate — append-only cross-cutting audit trail.

Stream ID format: audit-{entity_type}-{entity_id}

Design invariants:
  - Append-only: no delete or remove methods exist on this aggregate.
  - Cross-stream causal ordering is enforced via correlation_id chains stored
    in event metadata (the store layer, not here).
  - The integrity_checks list grows monotonically; it is never shrunk.
"""
from __future__ import annotations
from dataclasses import dataclass, field

from ledger.schema.events import StoredEvent


@dataclass
class AuditLedgerAggregate:
    entity_type: str
    entity_id: str
    events_count: int = 0
    last_integrity_hash: str | None = None
    integrity_checks: list[dict] = field(default_factory=list)
    _version: int = field(default=0, repr=False)

    @classmethod
    async def load(
        cls,
        store,
        entity_type: str,
        entity_id: str,
    ) -> "AuditLedgerAggregate":
        """Replay the audit stream to rebuild aggregate state."""
        stream_id = f"audit-{entity_type}-{entity_id}"
        events = await store.load_stream(stream_id)
        agg = cls(entity_type=entity_type, entity_id=entity_id)
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: StoredEvent) -> None:
        """
        Dispatch to _on_<EventType> via getattr.  Unknown event types are
        silently ignored for forward-compatibility (audit stream may receive
        new event types without a code deploy).
        """
        handler_name = f"_on_{event.event_type}"
        handler = getattr(self, handler_name, None)
        if handler is not None:
            handler(event)
        self._version = event.stream_position

    # ─── EVENT HANDLERS ───────────────────────────────────────────────────────

    def _on_AuditIntegrityCheckRun(self, event: StoredEvent) -> None:
        """
        Record the result of a periodic integrity sweep.
        Appends a new entry to integrity_checks — never modifies existing ones.
        """
        self.events_count = event.payload.get("events_verified_count", 0)
        self.last_integrity_hash = event.payload.get("integrity_hash")
        self.integrity_checks.append(
            {
                "check_timestamp": event.payload.get("check_timestamp"),
                "events_verified": event.payload.get("events_verified_count"),
                "hash": event.payload.get("integrity_hash"),
                "previous_hash": event.payload.get("previous_hash"),
            }
        )

    # ─── VERSION PROPERTY ─────────────────────────────────────────────────────

    @property
    def version(self) -> int:
        """Current stream position — used as expected_version in append()."""
        return self._version

    # ─── INTENTIONALLY ABSENT ─────────────────────────────────────────────────
    # delete() / remove() / clear() are NOT implemented.
    # This aggregate is append-only by design.  Any attempt to remove audit
    # entries would violate the regulatory immutability requirement.
