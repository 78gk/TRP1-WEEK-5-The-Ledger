"""
ledger/domain/aggregates/compliance_record.py
==============================================
ComplianceRecordAggregate — replays the compliance-{application_id} stream
to track regulatory check state for a loan application.

Stream ID format: compliance-{application_id}

All business rules live HERE. Command handlers only load, guard, and append.
"""
from __future__ import annotations
from dataclasses import dataclass, field

from ledger.schema.events import DomainError, StoredEvent


@dataclass
class ComplianceRecordAggregate:
    application_id: str
    checks_required: list[str] = field(default_factory=list)
    checks_passed: dict[str, dict] = field(default_factory=dict)
    checks_failed: dict[str, dict] = field(default_factory=dict)
    checks_noted: dict[str, dict] = field(default_factory=dict)
    overall_verdict: str | None = None
    has_hard_block: bool = False
    regulation_set_version: str | None = None
    _version: int = field(default=-1, repr=False)

    @classmethod
    async def load(cls, store, application_id: str) -> "ComplianceRecordAggregate":
        """
        Load and replay the compliance stream to rebuild aggregate state.

        ComplianceCheckRequested is emitted on the primary loan stream, so we
        merge that trigger event into the replay for rule-completeness checks.
        """
        stream_id = f"compliance-{application_id}"
        events = list(await store.load_stream(stream_id))
        upstream = [
            event
            for event in await store.load_stream(f"loan-{application_id}")
            if event.event_type == "ComplianceCheckRequested"
        ]
        events.extend(upstream)
        events.sort(key=lambda event: getattr(event, "global_position", -1))
        agg = cls(application_id=application_id)
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: StoredEvent) -> None:
        """
        Dispatch to the matching _on_<EventType> handler via getattr.
        Silently ignores unrecognised event types (forward-compatibility).
        Version is always advanced, even for unknown events.
        """
        handler_name = f"_on_{event.event_type}"
        handler = getattr(self, handler_name, None)
        if handler is not None:
            handler(event)
        if getattr(event, "stream_id", "").startswith("compliance-"):
            self._version = event.stream_position

    # ─── EVENT HANDLERS ───────────────────────────────────────────────────────

    def _on_ComplianceCheckRequested(self, event: StoredEvent) -> None:
        """Initialise the compliance record from the upstream loan stream trigger."""
        self.application_id = event.payload["application_id"]
        self.regulation_set_version = event.payload.get("regulation_set_version")
        self.checks_required = list(event.payload.get("rules_to_evaluate", []))

    def _on_ComplianceCheckInitiated(self, event: StoredEvent) -> None:
        """The compliance agent has started; record application_id (idempotent)."""
        self.application_id = event.payload["application_id"]

    def _on_ComplianceRulePassed(self, event: StoredEvent) -> None:
        rule_id = event.payload["rule_id"]
        self.checks_passed[rule_id] = {
            "rule_version": event.payload.get("rule_version"),
            "evidence": event.payload.get("evidence_hash"),
            "timestamp": event.payload.get("evaluated_at"),
        }

    def _on_ComplianceRuleFailed(self, event: StoredEvent) -> None:
        rule_id = event.payload["rule_id"]
        self.checks_failed[rule_id] = {
            "rule_version": event.payload.get("rule_version"),
            "failure_reason": event.payload.get("failure_reason"),
            "is_hard_block": event.payload.get("is_hard_block", False),
        }
        if event.payload.get("is_hard_block"):
            self.has_hard_block = True

    def _on_ComplianceRuleNoted(self, event: StoredEvent) -> None:
        rule_id = event.payload["rule_id"]
        self.checks_noted[rule_id] = {
            "note_type": event.payload.get("note_type"),
        }

    def _on_ComplianceCheckCompleted(self, event: StoredEvent) -> None:
        self.overall_verdict = event.payload.get("overall_verdict")

    # ─── GUARDS / BUSINESS RULES ──────────────────────────────────────────────

    def assert_all_mandatory_checks_complete(self) -> None:
        """
        Cannot issue compliance clearance without all mandatory checks evaluated.
        A check is considered evaluated if it appears in passed, failed, OR noted.
        """
        for rule_id in self.checks_required:
            if (
                rule_id not in self.checks_passed
                and rule_id not in self.checks_failed
                and rule_id not in self.checks_noted
            ):
                raise DomainError(
                    f"Mandatory check {rule_id!r} not yet evaluated. "
                    f"All required checks must complete before compliance verdict."
                )

    def assert_no_hard_block(self) -> None:
        """Prevent downstream steps when a hard compliance block is active."""
        if self.has_hard_block:
            raise DomainError(
                "Cannot proceed: compliance hard block active. "
                "A hard-block rule failure prohibits any approval path."
            )

    def assert_rule_is_expected(self, rule_id: str) -> None:
        if self.checks_required and rule_id not in self.checks_required:
            raise DomainError(
                f"Rule {rule_id!r} is not part of the active regulation set "
                f"for application {self.application_id!r}."
            )

    def assert_decision_allowed(self, recommendation: str) -> None:
        self.assert_all_mandatory_checks_complete()
        if recommendation.upper() != "DECLINE":
            self.assert_no_hard_block()

    def is_clear(self) -> bool:
        """Return True iff the overall compliance verdict is CLEAR."""
        return self.overall_verdict == "CLEAR"

    # ─── VERSION PROPERTY ─────────────────────────────────────────────────────

    @property
    def version(self) -> int:
        """Current stream position — used as expected_version in append()."""
        return self._version
