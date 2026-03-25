from __future__ import annotations

from ledger.commands.handlers import (
    handle_compliance_check,
    handle_credit_analysis_completed,
    handle_fraud_screening_completed,
    handle_generate_decision,
    handle_human_review_completed,
    handle_start_agent_session,
    handle_submit_application,
)
from ledger.integrity.audit_chain import run_integrity_check as run_integrity_check_command
from ledger.schema.events import DomainError, OptimisticConcurrencyError


def _error(error_type: str, message: str, suggested_action: str, **context) -> dict:
    return {
        "error_type": error_type,
        "message": message,
        "suggested_action": suggested_action,
        **context,
    }


def register_tools(mcp, event_store):
    @mcp.tool(description="""
    Submit a new loan application. Creates a new loan stream.

    PRECONDITIONS: None - this is the entry point for a new application.

    RETURNS: {stream_id, initial_version} on success.
    ERRORS:
    - DuplicateApplicationError if application_id already exists.
    - ValidationError if required fields are missing or invalid.
    """)
    async def submit_application(
        application_id: str,
        applicant_id: str,
        requested_amount_usd: float,
        loan_purpose: str,
    ) -> dict:
        try:
            version = await handle_submit_application(
                event_store,
                application_id,
                applicant_id,
                requested_amount_usd,
                loan_purpose,
            )
            return {
                "stream_id": f"loan-{application_id}",
                "initial_version": version,
            }
        except OptimisticConcurrencyError as exc:
            return _error(
                "DuplicateApplicationError",
                f"Application {application_id} already exists",
                "use_different_application_id",
                stream_id=exc.stream_id,
                expected_version=exc.expected_version,
                actual_version=exc.actual_version,
            )
        except Exception as exc:
            return _error(
                "ValidationError",
                str(exc),
                "check_required_fields",
                application_id=application_id,
            )

    @mcp.tool(description="""
    Record a completed credit analysis for a loan application.

    PRECONDITIONS:
    - An active agent session MUST exist and have its context loaded.
    - The loan application must be in a state awaiting credit analysis.

    RETURNS: {event_id, new_stream_version} on success.
    ERRORS:
    - PreconditionFailed if no active agent session can satisfy the handler preconditions.
    - OptimisticConcurrencyError if the stream was modified concurrently.
    - DomainError if the application is in the wrong state.
    """)
    async def record_credit_analysis(
        application_id: str,
        agent_id: str,
        session_id: str,
        model_version: str,
        confidence_score: float,
        risk_tier: str,
        recommended_limit_usd: float,
        rationale: str = "",
        model_deployment_id: str = "generated",
        input_data_hash: str = "generated",
        analysis_duration_ms: int = 0,
        regulatory_basis: list[str] | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> dict:
        decision_dict = {
            "risk_tier": risk_tier,
            "recommended_limit_usd": recommended_limit_usd,
            "confidence": confidence_score,
            "rationale": rationale,
        }
        try:
            version = await handle_credit_analysis_completed(
                event_store,
                application_id,
                agent_id,
                session_id,
                decision_dict,
                model_version,
                model_deployment_id,
                input_data_hash,
                analysis_duration_ms,
                regulatory_basis=regulatory_basis,
                correlation_id=correlation_id,
                causation_id=causation_id,
            )
            return {"event_id": "generated", "new_stream_version": version}
        except OptimisticConcurrencyError as exc:
            return _error(
                "OptimisticConcurrencyError",
                f"Stream {exc.stream_id} was modified",
                "reload_stream_and_retry",
                stream_id=exc.stream_id,
                expected_version=exc.expected_version,
                actual_version=exc.actual_version,
            )
        except DomainError as exc:
            return _error(
                "DomainError",
                str(exc),
                "check_application_state",
                application_id=application_id,
                session_id=session_id,
            )
        except Exception as exc:
            return _error(
                "PreconditionFailed",
                str(exc),
                "start_agent_session_and_load_context",
                application_id=application_id,
                session_id=session_id,
            )

    @mcp.tool(description="""
    Record fraud screening results.

    PRECONDITIONS:
    - An active agent session MUST exist and have its context loaded.
    - The application must have credit analysis completed.

    RETURNS: {event_id, new_stream_version} on success.
    ERRORS:
    - PreconditionFailed if no active agent session can satisfy the handler preconditions.
    - OptimisticConcurrencyError if the stream was modified concurrently.
    - DomainError if the application is in the wrong state.
    """)
    async def record_fraud_screening(
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
    ) -> dict:
        try:
            version = await handle_fraud_screening_completed(
                event_store,
                application_id,
                agent_id,
                session_id,
                fraud_score,
                risk_level,
                anomalies_found,
                recommendation,
                screening_model_version,
                input_data_hash,
                correlation_id=correlation_id,
                causation_id=causation_id,
            )
            return {"event_id": "generated", "new_stream_version": version}
        except OptimisticConcurrencyError as exc:
            return _error(
                "OptimisticConcurrencyError",
                f"Stream {exc.stream_id} was modified",
                "reload_stream_and_retry",
                stream_id=exc.stream_id,
                expected_version=exc.expected_version,
                actual_version=exc.actual_version,
            )
        except DomainError as exc:
            return _error(
                "DomainError",
                str(exc),
                "check_application_state",
                application_id=application_id,
                session_id=session_id,
            )
        except Exception as exc:
            return _error(
                "PreconditionFailed",
                str(exc),
                "start_agent_session_and_load_context",
                application_id=application_id,
                session_id=session_id,
            )

    @mcp.tool(description="""
    Record a compliance rule evaluation.

    PRECONDITIONS:
    - An active compliance workflow must already be in progress for the application.
    - rule_id must be valid for the regulation set being evaluated.

    RETURNS: {event_id, new_stream_version} on success.
    ERRORS:
    - DomainError if the application state or rule inputs are invalid.
    - OptimisticConcurrencyError if the compliance stream was modified concurrently.
    """)
    async def record_compliance_check(
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
    ) -> dict:
        try:
            version = await handle_compliance_check(
                event_store,
                application_id,
                session_id,
                rule_id,
                rule_name,
                rule_version,
                passed,
                failure_reason=failure_reason,
                is_hard_block=is_hard_block,
                remediation_available=remediation_available,
                remediation_description=remediation_description,
                evidence_hash=evidence_hash,
                note_type=note_type,
                correlation_id=correlation_id,
                causation_id=causation_id,
            )
            return {"event_id": "generated", "new_stream_version": version}
        except OptimisticConcurrencyError as exc:
            return _error(
                "OptimisticConcurrencyError",
                f"Stream {exc.stream_id} was modified",
                "reload_stream_and_retry",
                stream_id=exc.stream_id,
                expected_version=exc.expected_version,
                actual_version=exc.actual_version,
            )
        except DomainError as exc:
            return _error(
                "DomainError",
                str(exc),
                "check_rule_id_and_application_state",
                application_id=application_id,
                rule_id=rule_id,
            )

    @mcp.tool(description="""
    Generate a loan decision (APPROVE, DECLINE, or REFER).

    PRECONDITIONS:
    - Credit analysis, fraud screening, and compliance prerequisites must be complete.
    - If confidence is below 0.60, the aggregate logic may reject APPROVE and require REFER.

    RETURNS: {event_id, new_stream_version} on success.
    ERRORS:
    - DomainError if prerequisites are incomplete or recommendation conflicts with policy.
    - OptimisticConcurrencyError if the stream was modified concurrently.
    """)
    async def generate_decision(
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
    ) -> dict:
        try:
            version = await handle_generate_decision(
                event_store,
                application_id,
                orchestrator_session_id,
                recommendation,
                confidence,
                approved_amount_usd,
                conditions,
                executive_summary,
                key_risks,
                contributing_sessions,
                model_versions=model_versions,
                correlation_id=correlation_id,
                causation_id=causation_id,
            )
            return {"event_id": "generated", "new_stream_version": version}
        except OptimisticConcurrencyError as exc:
            return _error(
                "OptimisticConcurrencyError",
                f"Stream {exc.stream_id} was modified",
                "reload_stream_and_retry",
                stream_id=exc.stream_id,
                expected_version=exc.expected_version,
                actual_version=exc.actual_version,
            )
        except DomainError as exc:
            return _error(
                "DomainError",
                str(exc),
                "complete_prerequisites_then_retry",
                application_id=application_id,
                orchestrator_session_id=orchestrator_session_id,
            )

    @mcp.tool(description="""
    Record a human loan officer's review decision.

    PRECONDITIONS:
    - A decision must already have been generated for the application.
    - If override is true, override_reason is REQUIRED.

    RETURNS: {final_decision, application_state} on success.
    ERRORS:
    - DomainError if no reviewable decision exists.
    - ValidationError if override is true and override_reason is missing.
    - OptimisticConcurrencyError if the stream was modified concurrently.
    """)
    async def record_human_review(
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
    ) -> dict:
        if override and not override_reason:
            return _error(
                "ValidationError",
                "override_reason is required when override is true",
                "provide_override_reason",
                application_id=application_id,
                reviewer_id=reviewer_id,
            )
        try:
            await handle_human_review_completed(
                event_store,
                application_id,
                reviewer_id,
                override,
                original_recommendation,
                final_decision,
                override_reason=override_reason,
                approved_amount_usd=approved_amount_usd,
                interest_rate_pct=interest_rate_pct,
                term_months=term_months,
                conditions=conditions,
                decline_reasons=decline_reasons,
                correlation_id=correlation_id,
                causation_id=causation_id,
            )
            return {
                "final_decision": final_decision,
                "application_state": final_decision.upper(),
            }
        except OptimisticConcurrencyError as exc:
            return _error(
                "OptimisticConcurrencyError",
                f"Stream {exc.stream_id} was modified",
                "reload_stream_and_retry",
                stream_id=exc.stream_id,
                expected_version=exc.expected_version,
                actual_version=exc.actual_version,
            )
        except DomainError as exc:
            return _error(
                "DomainError",
                str(exc),
                "generate_decision_before_review",
                application_id=application_id,
            )

    @mcp.tool(description="""
    Initialize an agent session. REQUIRED before any agent decision tools.

    PRECONDITIONS: None - this creates a new session stream using the Gas Town pattern.

    RETURNS: {session_id, context_position} on success.
    ERRORS:
    - DuplicateSessionError if the target session stream already exists.
    - ValidationError if required fields are missing or invalid.
    """)
    async def start_agent_session(
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
    ) -> dict:
        try:
            version = await handle_start_agent_session(
                event_store,
                session_id,
                agent_type,
                agent_id,
                application_id,
                model_version,
                langgraph_graph_version,
                context_source,
                context_token_count,
                correlation_id=correlation_id,
                causation_id=causation_id,
            )
            return {"session_id": session_id, "context_position": version}
        except OptimisticConcurrencyError as exc:
            return _error(
                "DuplicateSessionError",
                f"Agent session {session_id} already exists",
                "use_different_session_id",
                stream_id=exc.stream_id,
                expected_version=exc.expected_version,
                actual_version=exc.actual_version,
            )
        except Exception as exc:
            return _error(
                "ValidationError",
                str(exc),
                "check_required_fields",
                session_id=session_id,
                agent_type=agent_type,
            )

    @mcp.tool(description="""
    Run cryptographic integrity verification on an entity's event history.

    PRECONDITIONS:
    - The entity stream must already exist.
    - Operational policy should rate-limit checks to at most once per minute per entity.

    RETURNS: {check_result: {chain_valid, tamper_detected, events_verified}} on success.
    ERRORS:
    - NotFoundError if the entity stream does not exist.
    - ValidationError if the entity identifiers are invalid.
    """)
    async def run_integrity_check(entity_type: str, entity_id: str) -> dict:
        stream_id = f"{entity_type}-{entity_id}"
        if await event_store.stream_version(stream_id) == -1:
            return _error(
                "NotFoundError",
                f"Entity stream {stream_id} does not exist",
                "check_entity_type_and_entity_id",
                stream_id=stream_id,
            )
        try:
            result = await run_integrity_check_command(event_store, entity_type, entity_id)
            return {
                "check_result": {
                    "chain_valid": result.chain_valid,
                    "tamper_detected": result.tamper_detected,
                    "events_verified": result.events_verified_count,
                    "integrity_hash": result.integrity_hash,
                    "previous_hash": result.previous_hash,
                }
            }
        except Exception as exc:
            return _error(
                "ValidationError",
                str(exc),
                "check_entity_inputs",
                entity_type=entity_type,
                entity_id=entity_id,
            )

    return None
