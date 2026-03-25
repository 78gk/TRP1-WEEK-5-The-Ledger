"""
ledger/commands/handlers.py
============================
Command handlers for the Ledger domain.

Every handler follows the EXACT four-step pattern:
  1. Load aggregate(s) from event store (event replay — no raw DB queries)
  2. Call guard methods on aggregate(s) — ALL validation delegated to aggregates
  3. Construct event objects — NO database calls in this step
  4. Call event_store.append() with expected_version from loaded aggregate's .version

Critical rules enforced here:
  - expected_version ALWAYS comes from loaded aggregate's .version property — NEVER hardcoded
  - correlation_id and causation_id are accepted on EVERY handler and passed to append()
  - Business conditionals live in aggregate guard methods, NOT here
"""
from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from ledger.event_store import EventStore
from ledger.schema.events import (
    AgentSessionStarted,
    AgentType,
    ApplicationSubmitted,
    ComplianceCheckCompleted,
    ComplianceCheckRequested,
    ComplianceRuleFailed,
    ComplianceRulePassed,
    ComplianceVerdict,
    CreditAnalysisRequested,
    CreditAnalysisCompleted,
    CreditDecision,
    DecisionGenerated,
    DecisionRequested,
    FraudScreeningCompleted,
    HumanReviewCompleted,
    ApplicationApproved,
    ApplicationDeclined,
    LoanPurpose,
    RiskTier,
)
from ledger.domain.aggregates.loan_application import LoanApplicationAggregate
from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.domain.aggregates.compliance_record import ComplianceRecordAggregate


def _parse_session_reference(session_ref: str) -> tuple[str, str]:
    parts = session_ref.split("-", 2)
    if len(parts) != 3 or parts[0] != "agent":
        raise DomainError(f"Invalid contributing session reference: {session_ref!r}")
    return parts[1], parts[2]


# ─── HANDLER 1: Submit Application ───────────────────────────────────────────

async def handle_submit_application(
    event_store: EventStore,
    application_id: str,
    applicant_id: str,
    requested_amount_usd: float,
    loan_purpose: str,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> int:
    """
    Handles the initial submission of a loan application.
    """
    # Step 1: No aggregate to load for new submission

    # Step 2: No guards for initial submission

    # Step 3: Construct event (no database calls)
    event = ApplicationSubmitted(
        application_id=application_id,
        applicant_id=applicant_id,
        requested_amount_usd=requested_amount_usd,
        loan_purpose=loan_purpose,
        loan_term_months=24,  # default or required by model
        submission_channel="api",
        contact_email="system@example.com",
        contact_name="System Submitted",
        submitted_at=datetime.utcnow(),
        application_reference=f"REF-{application_id}",
    )

    # Step 4: Append to event store
    return await event_store.append(
        stream_id=f"loan-{application_id}",
        events=[event],
        expected_version=-1,  # -1 because this is a new stream creation
        correlation_id=correlation_id,
        causation_id=causation_id,
    )


# ─── HANDLER 2: Credit Analysis Completed ────────────────────────────────────

async def handle_request_credit_analysis(
    event_store: EventStore,
    application_id: str,
    requested_by: str = "system",
    priority: str = "NORMAL",
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> int:
    app = await LoanApplicationAggregate.load(event_store, application_id)
    app.assert_state("SUBMITTED")

    event = CreditAnalysisRequested(
        application_id=application_id,
        requested_at=datetime.utcnow(),
        requested_by=requested_by,
        priority=priority,
    )

    return await event_store.append(
        stream_id=f"loan-{application_id}",
        events=[event],
        expected_version=app.version,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )


# ─── HANDLER 2: Credit Analysis Completed ────────────────────────────────────

async def handle_credit_analysis_completed(
    event_store: EventStore,
    application_id: str,
    agent_id: str,
    session_id: str,
    decision_dict: dict,
    model_version: str,
    model_deployment_id: str,
    input_data_hash: str,
    analysis_duration_ms: int,
    regulatory_basis: list[str] | None = None,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> int:
    """
    Handles exactly-once recording of a completed credit analysis by an AI agent.
    """
    # Step 1: Load BOTH aggregates
    app = await LoanApplicationAggregate.load(event_store, application_id)
    agent = await AgentSessionAggregate.load(event_store, agent_id, session_id)

    # Step 2: Call guards on BOTH
    app.assert_awaiting_credit_analysis()
    agent.assert_context_loaded()

    # Also enforce model version
    agent.assert_model_version_current(model_version)

    # Step 3: Construct event (no DB calls)
    decision_vo = CreditDecision(
        risk_tier=RiskTier(decision_dict.get("risk_tier", "MEDIUM")),
        recommended_limit_usd=decision_dict.get("recommended_limit_usd", 0.0),
        confidence=decision_dict.get("confidence", 0.0),
        rationale=decision_dict.get("rationale", ""),
        key_concerns=decision_dict.get("key_concerns", []),
        data_quality_caveats=decision_dict.get("data_quality_caveats", []),
        policy_overrides_applied=decision_dict.get("policy_overrides_applied", []),
    )

    event = CreditAnalysisCompleted(
        application_id=application_id,
        session_id=session_id,
        decision=decision_vo,
        model_version=model_version,
        model_deployment_id=model_deployment_id,
        input_data_hash=input_data_hash,
        analysis_duration_ms=analysis_duration_ms,
        regulatory_basis=regulatory_basis or [],
        completed_at=datetime.utcnow(),
    )

    # Step 4: Append with aggregate's tracked version
    return await event_store.append(
        stream_id=f"loan-{application_id}",
        events=[event],
        expected_version=app.version,  # FROM LOADED AGGREGATE
        correlation_id=correlation_id,
        causation_id=causation_id,
    )


# ─── HANDLER 2B: Request Compliance Review ───────────────────────────────────

async def handle_request_compliance_review(
    event_store: EventStore,
    application_id: str,
    regulation_set_version: str,
    rules_to_evaluate: list[str],
    triggered_by_event_id: str = "system",
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> int:
    app = await LoanApplicationAggregate.load(event_store, application_id)
    fraud_events = await event_store.load_stream(f"fraud-{application_id}")
    app.assert_ready_for_compliance_review(fraud_screening_completed=bool(fraud_events))

    event = ComplianceCheckRequested(
        application_id=application_id,
        requested_at=datetime.utcnow(),
        triggered_by_event_id=triggered_by_event_id,
        regulation_set_version=regulation_set_version,
        rules_to_evaluate=rules_to_evaluate,
    )

    return await event_store.append(
        stream_id=f"loan-{application_id}",
        events=[event],
        expected_version=app.version,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )


# ─── HANDLER 3: Fraud Screening Completed ────────────────────────────────────

async def handle_fraud_screening_completed(
    event_store: EventStore,
    application_id: str,
    agent_id: str,
    session_id: str,
    fraud_score: float,
    risk_level: str,
    anomalies_found: int,
    recommendation: str,
    screening_model_version: str,
    input_data_hash: str,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> int:
    """
    Records the outcome of a fraud screening pass by an AI agent.

    Guards (delegated to aggregates):
    - Application must have completed credit analysis (state ANALYSIS_COMPLETE).
    - Agent session must have declared its context (context_loaded guard).
    """
    # Step 1: Load aggregates from event replay
    app = await LoanApplicationAggregate.load(event_store, application_id)
    agent = await AgentSessionAggregate.load(event_store, agent_id, session_id)

    # Step 2: Guards — delegated entirely to aggregates
    app.assert_ready_for_fraud_screening()
    agent.assert_context_loaded()

    # Step 3: Construct event (no DB calls)
    event = FraudScreeningCompleted(
        application_id=application_id,
        session_id=session_id,
        fraud_score=fraud_score,
        risk_level=risk_level,
        anomalies_found=anomalies_found,
        recommendation=recommendation,
        screening_model_version=screening_model_version,
        input_data_hash=input_data_hash,
        completed_at=datetime.utcnow(),
    )

    # Step 4: Append with expected version from loaded aggregate
    fraud_version = await event_store.stream_version(f"fraud-{application_id}")
    return await event_store.append(
        stream_id=f"fraud-{application_id}",
        events=[event],
        expected_version=fraud_version,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )


# ─── HANDLER 4: Compliance Check (Rule Passed / Failed) ──────────────────────

async def handle_compliance_check(
    event_store: EventStore,
    application_id: str,
    session_id: str,
    rule_id: str,
    rule_name: str,
    rule_version: str,
    passed: bool,
    failure_reason: str | None = None,
    is_hard_block: bool = False,
    remediation_available: bool = False,
    remediation_description: str | None = None,
    evidence_hash: str = "",
    note_type: str | None = None,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> int:
    """
    Records the pass/fail outcome of a single compliance rule evaluation.

    Appends either ComplianceRulePassed or ComplianceRuleFailed to the
    compliance-{application_id} stream.

    Guards (delegated to aggregates):
    - Application must be in COMPLIANCE_REVIEW state.
    """
    # Step 1: Load BOTH aggregates from event replay
    app = await LoanApplicationAggregate.load(event_store, application_id)
    compliance_agg = await ComplianceRecordAggregate.load(event_store, application_id)

    # Step 2: Guard — application must be in compliance review phase
    app.assert_state("COMPLIANCE_REVIEW")
    compliance_agg.assert_rule_is_expected(rule_id)

    # Step 3: Construct the appropriate event (no DB calls)
    if passed:
        event = ComplianceRulePassed(
            application_id=application_id,
            session_id=session_id,
            rule_id=rule_id,
            rule_name=rule_name,
            rule_version=rule_version,
            evidence_hash=evidence_hash,
            evaluation_notes="",
            evaluated_at=datetime.utcnow(),
        )
    else:
        event = ComplianceRuleFailed(
            application_id=application_id,
            session_id=session_id,
            rule_id=rule_id,
            rule_name=rule_name,
            rule_version=rule_version,
            failure_reason=failure_reason or "Unspecified failure",
            is_hard_block=is_hard_block,
            remediation_available=remediation_available,
            remediation_description=remediation_description,
            evidence_hash=evidence_hash,
            evaluated_at=datetime.utcnow(),
        )

    # Step 4: Append to the compliance stream with compliance aggregate version
    return await event_store.append(
        stream_id=f"compliance-{application_id}",
        events=[event],
        expected_version=compliance_agg.version,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )


# ─── HANDLER 4B: Complete Compliance Review ──────────────────────────────────

async def handle_complete_compliance_review(
    event_store: EventStore,
    application_id: str,
    session_id: str,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> int:
    compliance_agg = await ComplianceRecordAggregate.load(event_store, application_id)
    compliance_agg.assert_all_mandatory_checks_complete()

    passed = len(compliance_agg.checks_passed)
    failed = len(compliance_agg.checks_failed)
    noted = len(compliance_agg.checks_noted)
    overall_verdict = (
        ComplianceVerdict.BLOCKED
        if compliance_agg.has_hard_block
        else ComplianceVerdict.CLEAR
    )

    event = ComplianceCheckCompleted(
        application_id=application_id,
        session_id=session_id,
        rules_evaluated=passed + failed + noted,
        rules_passed=passed,
        rules_failed=failed,
        rules_noted=noted,
        has_hard_block=compliance_agg.has_hard_block,
        overall_verdict=overall_verdict,
        completed_at=datetime.utcnow(),
    )

    return await event_store.append(
        stream_id=f"compliance-{application_id}",
        events=[event],
        expected_version=compliance_agg.version,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )


# ─── HANDLER 4C: Request Decision ────────────────────────────────────────────

async def handle_request_decision(
    event_store: EventStore,
    application_id: str,
    triggered_by_event_id: str = "system",
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> int:
    app = await LoanApplicationAggregate.load(event_store, application_id)
    compliance_agg = await ComplianceRecordAggregate.load(event_store, application_id)

    app.assert_ready_to_request_decision(compliance_agg)

    event = DecisionRequested(
        application_id=application_id,
        requested_at=datetime.utcnow(),
        all_analyses_complete=True,
        triggered_by_event_id=triggered_by_event_id,
    )

    return await event_store.append(
        stream_id=f"loan-{application_id}",
        events=[event],
        expected_version=app.version,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )


# ─── HANDLER 5: Generate Decision ────────────────────────────────────────────

async def handle_generate_decision(
    event_store: EventStore,
    application_id: str,
    orchestrator_session_id: str,
    recommendation: str,
    confidence: float,
    approved_amount_usd: float | None,
    conditions: list[str],
    executive_summary: str,
    key_risks: list[str],
    contributing_sessions: list[str],
    model_versions: dict[str, str] | None = None,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> int:
    """
    Records the AI orchestrator's final decision recommendation.

    Business rules enforced (via aggregate guards):
    - Rule 4: confidence < 0.60 forces recommendation to REFER — enforced in
              LoanApplicationAggregate.assert_valid_orchestrator_decision()
    - Rule 5: Compliance BLOCKED → only DECLINE allowed — enforced in
              ComplianceRecordAggregate; if has_hard_block is True, assert_no_hard_block()
              raises DomainError unless recommendation is DECLINE.
    - Rule 6: Each contributing_session must reference a real, completed agent
              session for this application_id — verified below by loading each stream.
    """
    # Step 1: Load LoanApplication and ComplianceRecord aggregates
    app = await LoanApplicationAggregate.load(event_store, application_id)
    compliance_agg = await ComplianceRecordAggregate.load(event_store, application_id)

    # Step 2: Guard calls — all validation delegated to aggregates
    # Rule 4: confidence threshold and recommendation consistency
    app.assert_valid_orchestrator_decision(recommendation, confidence)

    # Rule 5: compliance dependency and hard-block behavior
    compliance_agg.assert_decision_allowed(recommendation)

    # Rule 6: Validate that each contributing session stream exists and contains
    #         a valid decision-session causal chain for this application.
    sessions: list[AgentSessionAggregate] = []
    for session_ref in contributing_sessions:
        agent_type, session_id = _parse_session_reference(session_ref)
        session = await AgentSessionAggregate.load(event_store, agent_type, session_id)
        sessions.append(session)
    app.assert_valid_contributing_sessions(application_id, sessions)

    # Step 3: Construct event (no DB calls)
    event = DecisionGenerated(
        application_id=application_id,
        orchestrator_session_id=orchestrator_session_id,
        recommendation=recommendation,
        confidence=confidence,
        approved_amount_usd=approved_amount_usd,
        conditions=conditions,
        executive_summary=executive_summary,
        key_risks=key_risks,
        contributing_sessions=contributing_sessions,
        model_versions=model_versions or {},
        generated_at=datetime.utcnow(),
    )

    # Step 4: Append with expected version from loaded loan application aggregate
    return await event_store.append(
        stream_id=f"loan-{application_id}",
        events=[event],
        expected_version=app.version,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )


# ─── HANDLER 6: Human Review Completed ───────────────────────────────────────

async def handle_human_review_completed(
    event_store: EventStore,
    application_id: str,
    reviewer_id: str,
    override: bool,
    original_recommendation: str,
    final_decision: str,
    override_reason: str | None = None,
    approved_amount_usd: float | None = None,
    interest_rate_pct: float | None = None,
    term_months: int | None = None,
    conditions: list[str] | None = None,
    decline_reasons: list[str] | None = None,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> int:
    """
    Records the outcome of a human underwriter review.

    The guard ensures the application is currently awaiting human review
    (i.e. a DecisionGenerated event was previously appended).

    If override=True or the decision results in APPROVE/DECLINE, corresponding
    terminal events (ApplicationApproved / ApplicationDeclined) are appended
    in the same stream after HumanReviewCompleted, all within expected_version
    from the single loaded aggregate.
    """
    # Step 1: Load aggregate from event replay
    app = await LoanApplicationAggregate.load(event_store, application_id)

    # Step 2: Guard — application must be in PENDING_DECISION or APPROVED/DECLINED_PENDING_HUMAN
    app.assert_awaiting_human_review()

    # Step 3: Construct events (no DB calls)
    events: list = []

    review_event = HumanReviewCompleted(
        application_id=application_id,
        reviewer_id=reviewer_id,
        override=override,
        original_recommendation=original_recommendation,
        final_decision=final_decision,
        override_reason=override_reason,
        reviewed_at=datetime.utcnow(),
    )
    events.append(review_event)

    # Append terminal event based on final_decision
    final = final_decision.upper()
    if final == "APPROVE":
        events.append(
            ApplicationApproved(
                application_id=application_id,
                approved_amount_usd=approved_amount_usd or 0.0,
                interest_rate_pct=interest_rate_pct or 0.0,
                term_months=term_months or 0,
                conditions=conditions or [],
                approved_by=reviewer_id,
                effective_date=datetime.utcnow().strftime("%Y-%m-%d"),
                approved_at=datetime.utcnow(),
            )
        )
    elif final in {"DECLINE", "REFER"}:
        events.append(
            ApplicationDeclined(
                application_id=application_id,
                decline_reasons=decline_reasons or ["Human reviewer declined"],
                declined_by=reviewer_id,
                adverse_action_notice_required=True,
                adverse_action_codes=[],
                declined_at=datetime.utcnow(),
            )
        )

    # Step 4: Append all events atomically with expected_version from aggregate
    return await event_store.append(
        stream_id=f"loan-{application_id}",
        events=events,
        expected_version=app.version,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )


# ─── HANDLER 7: Start Agent Session ──────────────────────────────────────────

async def handle_start_agent_session(
    event_store: EventStore,
    session_id: str,
    agent_type: str,
    agent_id: str,
    application_id: str,
    model_version: str,
    langgraph_graph_version: str,
    context_source: str,
    context_token_count: int,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> int:
    """
    Creates a NEW AgentSession stream by appending AgentSessionStarted.

    This is a Gas Town pattern — the first event records the session's existence
    before any work begins.  A brand-new stream always has expected_version=-1.

    The stream_id format follows: agent-{agent_type}-{session_id}
    """
    # Step 1: No existing aggregate to load (new stream)

    # Step 2: No guards for new session creation

    # Step 3: Construct the session-start event (no DB calls)
    event = AgentSessionStarted(
        session_id=session_id,
        agent_type=AgentType(agent_type),
        agent_id=agent_id,
        application_id=application_id,
        model_version=model_version,
        langgraph_graph_version=langgraph_graph_version,
        context_source=context_source,
        context_token_count=context_token_count,
        started_at=datetime.utcnow(),
    )

    # Step 4: Append to new stream; expected_version=-1 signals new stream creation
    return await event_store.append(
        stream_id=f"agent-{agent_type}-{session_id}",
        events=[event],
        expected_version=-1,   # New stream — hardcoded -1 is correct here
        correlation_id=correlation_id,
        causation_id=causation_id,
    )
