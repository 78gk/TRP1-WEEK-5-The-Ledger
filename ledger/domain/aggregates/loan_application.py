"""
ledger/domain/aggregates/loan_application.py
=============================================
COMPLETION STATUS: STUB — implement apply() for each event, enforce business rules.

The aggregate replays its event stream to rebuild state.
Command handlers validate against current state before appending events.

BUSINESS RULES TO ENFORCE:
  1. State machine: only valid transitions allowed
  2. DocumentFactsExtracted must exist before CreditAnalysisCompleted
  3. All 6 compliance rules must complete before DecisionGenerated (unless hard block)
  4. confidence < 0.60 → recommendation must be REFER (enforced here, not in LLM)
  5. Compliance BLOCKED → only DECLINE allowed, not APPROVE or REFER
  6. Causal chain: every agent event must reference a triggering event_id

See: Section 4 of challenge document for full rule specifications.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum

class ApplicationState(str, Enum):
    NEW = "NEW"
    SUBMITTED = "SUBMITTED"
    AWAITING_ANALYSIS = "AWAITING_ANALYSIS"
    ANALYSIS_COMPLETE = "ANALYSIS_COMPLETE"
    COMPLIANCE_REVIEW = "COMPLIANCE_REVIEW"
    PENDING_DECISION = "PENDING_DECISION"
    APPROVED_PENDING_HUMAN = "APPROVED_PENDING_HUMAN"
    DECLINED_PENDING_HUMAN = "DECLINED_PENDING_HUMAN"
    FINAL_APPROVED = "FINAL_APPROVED"
    FINAL_DECLINED = "FINAL_DECLINED"

VALID_TRANSITIONS = {
    ApplicationState.NEW: [ApplicationState.SUBMITTED],
    ApplicationState.SUBMITTED: [ApplicationState.AWAITING_ANALYSIS],
    ApplicationState.AWAITING_ANALYSIS: [ApplicationState.ANALYSIS_COMPLETE],
    ApplicationState.ANALYSIS_COMPLETE: [ApplicationState.COMPLIANCE_REVIEW],
    ApplicationState.COMPLIANCE_REVIEW: [ApplicationState.PENDING_DECISION],
    ApplicationState.PENDING_DECISION: [
        ApplicationState.APPROVED_PENDING_HUMAN,
        ApplicationState.DECLINED_PENDING_HUMAN,
    ],
    ApplicationState.APPROVED_PENDING_HUMAN: [ApplicationState.FINAL_APPROVED],
    ApplicationState.DECLINED_PENDING_HUMAN: [ApplicationState.FINAL_DECLINED],
}

# ─── DOMAIN IMPORTS ───────────────────────────────────────────────────────────
from ledger.schema.events import DomainError, StoredEvent


# ─── AGGREGATE ────────────────────────────────────────────────────────────────
@dataclass
class LoanApplicationAggregate:
    application_id: str
    state: ApplicationState = ApplicationState.NEW
    applicant_id: str | None = None
    requested_amount_usd: float | None = None
    loan_purpose: str | None = None
    credit_risk_tier: str | None = None
    credit_confidence: float | None = None
    version: int = 0  # Mirrors the stream's current_version

    @classmethod
    async def load(cls, event_store, application_id: str) -> "LoanApplicationAggregate":
        """Load and replay event stream to rebuild aggregate state."""
        agg = cls(application_id=application_id)
        stream_id = f"loan-{application_id}"
        events = await event_store.load_stream(stream_id)
        
        for event in events:
            agg._apply(event)
            
        return agg

    def _apply(self, event: StoredEvent) -> None:
        """
        Dynamically dispatches to _on_<EventType> based on the event's type.
        This avoids a monolithic if/elif block.
        """
        handler_name = f"_on_{event.event_type}"
        handler = getattr(self, handler_name, None)

        if handler:
            handler(event)
        self.version = event.stream_position  # Track stream version after replaying event
        # We ignore events we don't explicitly handle

    def _transition(self, target: ApplicationState) -> None:
        if target == self.state:
            return
        allowed = VALID_TRANSITIONS.get(self.state, [])
        if target not in allowed:
            raise DomainError(f"Invalid transition {self.state} -> {target}. Allowed: {allowed}")
        self.state = target

    # ─── EVENT HANDLERS ───────────────────────────────────────────────────────
    
    def _on_ApplicationSubmitted(self, event: StoredEvent) -> None:
        self._transition(ApplicationState.SUBMITTED)
        self.applicant_id = event.payload.get("applicant_id")
        self.requested_amount_usd = event.payload.get("requested_amount_usd")
        self.loan_purpose = event.payload.get("loan_purpose")

    def _on_CreditAnalysisRequested(self, event: StoredEvent) -> None:
        self._transition(ApplicationState.AWAITING_ANALYSIS)

    def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
        self._transition(ApplicationState.ANALYSIS_COMPLETE)

        # Pydantic v2 payload might be dict or object depending on upcasters, safely access via get or model dict
        payload = event.payload
        if isinstance(payload, dict):
            # Extract credit risk tier and confidence
            decision = payload.get("decision", {})
            if isinstance(decision, dict):
                self.credit_risk_tier = decision.get("risk_tier")
                self.credit_confidence = decision.get("confidence")
            else:
                self.credit_risk_tier = getattr(decision, "risk_tier", None)
                self.credit_confidence = getattr(decision, "confidence", None)

    def _on_ComplianceCheckRequested(self, event: StoredEvent) -> None:
        self._transition(ApplicationState.COMPLIANCE_REVIEW)

    def _on_DecisionRequested(self, event: StoredEvent) -> None:
        self._transition(ApplicationState.PENDING_DECISION)

    def _on_DecisionGenerated(self, event: StoredEvent) -> None:
        payload = event.payload if isinstance(event.payload, dict) else {}
        recommendation = str(payload.get("recommendation", "")).upper()
        if recommendation == "APPROVE":
            self._transition(ApplicationState.APPROVED_PENDING_HUMAN)
        elif recommendation in {"DECLINE", "REFER"}:
            self._transition(ApplicationState.DECLINED_PENDING_HUMAN)

    def _on_ApplicationApproved(self, event: StoredEvent) -> None:
        # Allow direct terminal approvals from automated path or approved-pending-human.
        if self.state == ApplicationState.PENDING_DECISION:
            self._transition(ApplicationState.APPROVED_PENDING_HUMAN)
        self._transition(ApplicationState.FINAL_APPROVED)

    def _on_ApplicationDeclined(self, event: StoredEvent) -> None:
        # Allow direct terminal declines from automated path or declined-pending-human.
        if self.state == ApplicationState.PENDING_DECISION:
            self._transition(ApplicationState.DECLINED_PENDING_HUMAN)
        self._transition(ApplicationState.FINAL_DECLINED)

    def _on_HumanReviewCompleted(self, event: StoredEvent) -> None:
        payload = event.payload if isinstance(event.payload, dict) else {}
        final_decision = str(payload.get("final_decision", "")).upper()
        if final_decision == "APPROVE":
            if self.state == ApplicationState.PENDING_DECISION:
                self._transition(ApplicationState.APPROVED_PENDING_HUMAN)
            self._transition(ApplicationState.FINAL_APPROVED)
        elif final_decision in {"DECLINE", "REFER"}:
            if self.state == ApplicationState.PENDING_DECISION:
                self._transition(ApplicationState.DECLINED_PENDING_HUMAN)
            self._transition(ApplicationState.FINAL_DECLINED)

    # ─── GUARDS / BUSINESS RULES ──────────────────────────────────────────────
    
    def assert_valid_transition(self, target: ApplicationState) -> None:
        allowed = VALID_TRANSITIONS.get(self.state, [])
        if target not in allowed:
            raise DomainError(f"Invalid transition {self.state} → {target}. Allowed: {allowed}")

    def assert_awaiting_credit_analysis(self) -> None:
        """Called before appending CreditAnalysisCompleted"""
        if self.state != ApplicationState.AWAITING_ANALYSIS:
            raise DomainError(
                f"Application must be locally in AWAITING_ANALYSIS before "
                f"accepting CreditAnalysisCompleted (currently {self.state.name})"
            )

    def assert_valid_orchestrator_decision(self, recommendation: str, confidence: float) -> None:
        """
        Enforce Rule 4: confidence < 0.60 → recommendation must be REFER.
        Raised during the command handler Phase before DecisionGenerated is emitted.
        """
        if confidence < 0.60 and recommendation.upper() == "APPROVE":
            raise DomainError(
                f"Cannot approve application: confidence {confidence:.2f} is below 0.60 threshold. "
                f"Recommendation must be REFER."
            )

    def assert_state(self, expected: str) -> None:
        if self.state.name != expected:
            raise DomainError(
                f"Application must be in {expected} state (currently {self.state.name})"
            )

    def assert_awaiting_human_review(self) -> None:
        if self.state not in {
            ApplicationState.PENDING_DECISION,
            ApplicationState.APPROVED_PENDING_HUMAN,
            ApplicationState.DECLINED_PENDING_HUMAN,
        }:
            raise DomainError(
                f"Application must be awaiting human review (currently {self.state.name})"
            )
