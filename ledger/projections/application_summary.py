"""
ledger/projections/application_summary.py
==========================================
Read-model projection: one row per loan application, always reflecting the
latest known state derived from domain events.

Registered with ProjectionDaemon — never called synchronously.
"""
from __future__ import annotations

from typing import Any

import asyncpg


# ── DDL ───────────────────────────────────────────────────────────────────────

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS application_summary (
    application_id          TEXT PRIMARY KEY,
    state                   TEXT,
    applicant_id            TEXT,
    requested_amount_usd    NUMERIC,
    approved_amount_usd     NUMERIC,
    risk_tier               TEXT,
    fraud_score             NUMERIC,
    compliance_status       TEXT,
    decision                TEXT,
    agent_sessions_completed TEXT[] DEFAULT '{}',
    last_event_type         TEXT,
    last_event_at           TIMESTAMPTZ,
    human_reviewer_id       TEXT,
    final_decision_at       TIMESTAMPTZ
);
"""


# ── Projection class ──────────────────────────────────────────────────────────

class ApplicationSummaryProjection:
    """
    UPSERT-based projection that maintains a single summary row per application.

    Subscribed events and their effect:
    - ApplicationSubmitted       → INSERT row, state = SUBMITTED
    - CreditAnalysisCompleted    → risk_tier
    - FraudScreeningCompleted    → fraud_score, state
    - ComplianceCheckCompleted   → compliance_status, state
    - DecisionGenerated          → decision, state = PENDING_DECISION
    - HumanReviewCompleted       → override decision, human_reviewer_id
    - ApplicationApproved        → approved_amount_usd, state = APPROVED
    - ApplicationDeclined        → state = DECLINED, final_decision_at
    """

    name = "application_summary"
    subscribed_events = [
        "ApplicationSubmitted",
        "CreditAnalysisCompleted",
        "FraudScreeningCompleted",
        "ComplianceCheckCompleted",
        "DecisionGenerated",
        "HumanReviewCompleted",
        "ApplicationApproved",
        "ApplicationDeclined",
    ]

    # ── Event handler ─────────────────────────────────────────────────────────

    async def handle(self, event: Any, pool: asyncpg.Pool) -> None:
        """Route event to the correct UPSERT handler."""
        et = event["event_type"] if isinstance(event, dict) else event.event_type
        p = event["payload"] if isinstance(event, dict) else event.payload
        recorded_at = (
            event.get("recorded_at") if isinstance(event, dict) else event.recorded_at
        )

        handler = getattr(self, f"_on_{_snake(et)}", None)
        if handler:
            async with pool.acquire() as conn:
                await handler(conn, p, recorded_at)

    # ── Per-event handlers (each receives an active connection) ───────────────

    async def _on_application_submitted(
        self, conn: asyncpg.Connection, p: dict, recorded_at: Any
    ) -> None:
        await conn.execute(
            """
            INSERT INTO application_summary
                (application_id, state, applicant_id, requested_amount_usd,
                 last_event_type, last_event_at)
            VALUES ($1, 'SUBMITTED', $2, $3, 'ApplicationSubmitted', $4)
            ON CONFLICT (application_id) DO UPDATE SET
                state            = 'SUBMITTED',
                applicant_id     = EXCLUDED.applicant_id,
                requested_amount_usd = EXCLUDED.requested_amount_usd,
                last_event_type  = 'ApplicationSubmitted',
                last_event_at    = EXCLUDED.last_event_at
            """,
            p["application_id"],
            p.get("applicant_id"),
            p.get("requested_amount_usd"),
            recorded_at,
        )

    async def _on_credit_analysis_completed(
        self, conn: asyncpg.Connection, p: dict, recorded_at: Any
    ) -> None:
        decision_payload = p.get("decision") or {}
        risk_tier = (
            decision_payload.get("risk_tier")
            if isinstance(decision_payload, dict)
            else getattr(decision_payload, "risk_tier", None)
        )
        await conn.execute(
            """
            INSERT INTO application_summary
                (application_id, risk_tier, last_event_type, last_event_at)
            VALUES ($1, $2, 'CreditAnalysisCompleted', $3)
            ON CONFLICT (application_id) DO UPDATE SET
                risk_tier       = EXCLUDED.risk_tier,
                last_event_type = 'CreditAnalysisCompleted',
                last_event_at   = EXCLUDED.last_event_at
            """,
            p.get("application_id"),
            str(risk_tier) if risk_tier else None,
            recorded_at,
        )

    async def _on_fraud_screening_completed(
        self, conn: asyncpg.Connection, p: dict, recorded_at: Any
    ) -> None:
        await conn.execute(
            """
            INSERT INTO application_summary
                (application_id, fraud_score, state, last_event_type, last_event_at)
            VALUES ($1, $2, 'FRAUD_SCREENING_COMPLETE', 'FraudScreeningCompleted', $3)
            ON CONFLICT (application_id) DO UPDATE SET
                fraud_score     = EXCLUDED.fraud_score,
                state           = 'FRAUD_SCREENING_COMPLETE',
                last_event_type = 'FraudScreeningCompleted',
                last_event_at   = EXCLUDED.last_event_at
            """,
            p.get("application_id"),
            p.get("fraud_score"),
            recorded_at,
        )

    async def _on_compliance_check_completed(
        self, conn: asyncpg.Connection, p: dict, recorded_at: Any
    ) -> None:
        await conn.execute(
            """
            INSERT INTO application_summary
                (application_id, compliance_status, state,
                 last_event_type, last_event_at)
            VALUES ($1, $2, 'COMPLIANCE_CHECK_COMPLETE',
                    'ComplianceCheckCompleted', $3)
            ON CONFLICT (application_id) DO UPDATE SET
                compliance_status = EXCLUDED.compliance_status,
                state             = 'COMPLIANCE_CHECK_COMPLETE',
                last_event_type   = 'ComplianceCheckCompleted',
                last_event_at     = EXCLUDED.last_event_at
            """,
            p.get("application_id"),
            p.get("overall_verdict"),
            recorded_at,
        )

    async def _on_decision_generated(
        self, conn: asyncpg.Connection, p: dict, recorded_at: Any
    ) -> None:
        await conn.execute(
            """
            INSERT INTO application_summary
                (application_id, decision, state, last_event_type, last_event_at)
            VALUES ($1, $2, 'PENDING_DECISION', 'DecisionGenerated', $3)
            ON CONFLICT (application_id) DO UPDATE SET
                decision        = EXCLUDED.decision,
                state           = 'PENDING_DECISION',
                last_event_type = 'DecisionGenerated',
                last_event_at   = EXCLUDED.last_event_at
            """,
            p.get("application_id"),
            p.get("recommendation"),
            recorded_at,
        )

    async def _on_human_review_completed(
        self, conn: asyncpg.Connection, p: dict, recorded_at: Any
    ) -> None:
        await conn.execute(
            """
            INSERT INTO application_summary
                (application_id, decision, human_reviewer_id,
                 last_event_type, last_event_at)
            VALUES ($1, $2, $3, 'HumanReviewCompleted', $4)
            ON CONFLICT (application_id) DO UPDATE SET
                decision          = EXCLUDED.decision,
                human_reviewer_id = EXCLUDED.human_reviewer_id,
                last_event_type   = 'HumanReviewCompleted',
                last_event_at     = EXCLUDED.last_event_at
            """,
            p.get("application_id"),
            p.get("final_decision"),
            p.get("reviewer_id"),
            recorded_at,
        )

    async def _on_application_approved(
        self, conn: asyncpg.Connection, p: dict, recorded_at: Any
    ) -> None:
        await conn.execute(
            """
            INSERT INTO application_summary
                (application_id, state, approved_amount_usd,
                 final_decision_at, last_event_type, last_event_at)
            VALUES ($1, 'APPROVED', $2, $3, 'ApplicationApproved', $3)
            ON CONFLICT (application_id) DO UPDATE SET
                state                = 'APPROVED',
                approved_amount_usd  = EXCLUDED.approved_amount_usd,
                final_decision_at    = EXCLUDED.final_decision_at,
                last_event_type      = 'ApplicationApproved',
                last_event_at        = EXCLUDED.last_event_at
            """,
            p.get("application_id"),
            p.get("approved_amount_usd"),
            recorded_at,
        )

    async def _on_application_declined(
        self, conn: asyncpg.Connection, p: dict, recorded_at: Any
    ) -> None:
        await conn.execute(
            """
            INSERT INTO application_summary
                (application_id, state, decision,
                 final_decision_at, last_event_type, last_event_at)
            VALUES ($1, 'DECLINED', 'DECLINED', $2, 'ApplicationDeclined', $2)
            ON CONFLICT (application_id) DO UPDATE SET
                state             = 'DECLINED',
                decision          = 'DECLINED',
                final_decision_at = EXCLUDED.final_decision_at,
                last_event_type   = 'ApplicationDeclined',
                last_event_at     = EXCLUDED.last_event_at
            """,
            p.get("application_id"),
            recorded_at,
        )

    # ── Rebuild ───────────────────────────────────────────────────────────────

    async def rebuild_from_scratch(self, pool: asyncpg.Pool) -> None:
        """
        Truncate the read-model table and reset the checkpoint to 0.
        The daemon will replay all events from the beginning on next startup.
        """
        async with pool.acquire() as conn:
            await conn.execute("TRUNCATE application_summary")
            await conn.execute(
                "UPDATE projection_checkpoints "
                "SET last_position = 0 "
                "WHERE projection_name = $1",
                self.name,
            )


# ── Internal helper ───────────────────────────────────────────────────────────

def _snake(event_type: str) -> str:
    """Convert 'ApplicationSubmitted' → 'application_submitted'."""
    import re
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", event_type)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
