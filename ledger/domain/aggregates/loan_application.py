"""
ledger/domain/aggregates/loan_application.py
=============================================
LoanApplicationAggregate is the authoritative state machine for the primary
loan stream. It rebuilds state from event replay and exposes guard methods used
by command handlers.

Business rules enforced through aggregate methods:
  1. state-machine transitions
  2. credit-analysis precondition before completion
  3. fraud screening before compliance review
  4. confidence < 0.60 forces REFER
  5. compliance completion before decision request/generation
  6. contributing session causal-chain validation for decisions
"""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum

from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.domain.aggregates.compliance_record import ComplianceRecordAggregate

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
    fraud_screening_completed: bool = False
    compliance_requested: bool = False
    decision_requested: bool = False
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

    def _on_FraudScreeningCompleted(self, event: StoredEvent) -> None:
        self.fraud_screening_completed = True

    def _on_ComplianceCheckRequested(self, event: StoredEvent) -> None:
        self.compliance_requested = True
        self._transition(ApplicationState.COMPLIANCE_REVIEW)

    def _on_ComplianceCheckCompleted(self, event: StoredEvent) -> None:
        self.compliance_requested = True

    def _on_DecisionRequested(self, event: StoredEvent) -> None:
        self.decision_requested = True
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
        if confidence < 0.60 and recommendation.upper() != "REFER":
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

    def assert_ready_for_fraud_screening(self) -> None:
        if self.state != ApplicationState.ANALYSIS_COMPLETE:
            raise DomainError(
                f"Application must be in ANALYSIS_COMPLETE before fraud screening "
                f"(currently {self.state.name})"
            )

    def assert_ready_for_compliance_review(
        self,
        fraud_screening_completed: bool | None = None,
    ) -> None:
        if self.state != ApplicationState.ANALYSIS_COMPLETE:
            raise DomainError(
                f"Application must still be in ANALYSIS_COMPLETE before compliance review "
                f"(currently {self.state.name})"
            )
        fraud_done = self.fraud_screening_completed if fraud_screening_completed is None else fraud_screening_completed
        if not fraud_done:
            raise DomainError(
                "Fraud screening must complete before compliance review can begin."
            )

    def assert_ready_to_request_decision(self, compliance: ComplianceRecordAggregate) -> None:
        if self.state != ApplicationState.COMPLIANCE_REVIEW:
            raise DomainError(
                f"Application must be in COMPLIANCE_REVIEW before decision request "
                f"(currently {self.state.name})"
            )
        compliance.assert_all_mandatory_checks_complete()

    def assert_valid_contributing_sessions(
        self,
        application_id: str,
        sessions: list[AgentSessionAggregate],
    ) -> None:
        if not sessions:
            raise DomainError("At least one contributing agent session is required.")
        for session in sessions:
            session.assert_referenced_for_decision(application_id)
