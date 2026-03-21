from datetime import datetime

from ledger.event_store import EventStore
from ledger.schema.events import ApplicationSubmitted, CreditAnalysisCompleted
from ledger.domain.aggregates.loan_application import LoanApplicationAggregate
from ledger.domain.aggregates.agent_session import AgentSessionAggregate


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
    from ledger.schema.events import CreditDecision, RiskTier
    
    # Helper to parse dictionary into the CreditDecision value object
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
