"""
ledger/projections/agent_performance.py
========================================
Read-model projection: per-(agent_id, model_version) performance metrics,
including human override rate, average confidence, and outcome distribution.

Registered with ProjectionDaemon — never called synchronously.
"""
from __future__ import annotations

from typing import Any

import asyncpg


# ── DDL ───────────────────────────────────────────────────────────────────────

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS agent_performance_ledger (
    agent_id                TEXT,
    model_version           TEXT,
    analyses_completed      INTEGER     DEFAULT 0,
    decisions_generated     INTEGER     DEFAULT 0,
    avg_confidence_score    NUMERIC,
    avg_duration_ms         NUMERIC,
    approve_rate            NUMERIC,
    decline_rate            NUMERIC,
    refer_rate              NUMERIC,
    human_override_count    INTEGER     DEFAULT 0,
    human_override_rate     NUMERIC,
    first_seen_at           TIMESTAMPTZ,
    last_seen_at            TIMESTAMPTZ,
    PRIMARY KEY (agent_id, model_version)
);
"""


# ── Projection class ──────────────────────────────────────────────────────────

class AgentPerformanceLedgerProjection:
    """
    Incremental aggregation of per-agent metrics as events arrive.

    Uses PostgreSQL UPSERT arithmetic to avoid SELECT-then-UPDATE races:
      - ``analyses_completed``, ``decisions_generated`` → simple increments
      - ``avg_confidence_score``, ``avg_duration_ms`` → incremental mean update
        via running-sum columns maintained in the DB
      - ``approve_rate / decline_rate / refer_rate`` → recomputed per-upsert
      - ``human_override_rate`` → overrides / (analyses_completed)
    """

    name = "agent_performance_ledger"
    subscribed_events = [
        "CreditAnalysisCompleted",
        "FraudScreeningCompleted",
        "DecisionGenerated",
        "HumanReviewCompleted",
        "AgentSessionCompleted",
    ]

    # ── Event handler ─────────────────────────────────────────────────────────

    async def handle(self, event: Any, pool: asyncpg.Pool) -> None:
        et = event["event_type"] if isinstance(event, dict) else event.event_type
        p = event["payload"] if isinstance(event, dict) else event.payload
        recorded_at = (
            event.get("recorded_at") if isinstance(event, dict) else event.recorded_at
        )

        dispatch = {
            "CreditAnalysisCompleted": self._on_credit_analysis_completed,
            "FraudScreeningCompleted": self._on_fraud_screening_completed,
            "DecisionGenerated": self._on_decision_generated,
            "HumanReviewCompleted": self._on_human_review_completed,
            "AgentSessionCompleted": self._on_agent_session_completed,
        }
        handler = dispatch.get(et)
        if handler:
            async with pool.acquire() as conn:
                await handler(conn, p, recorded_at)

    # ── Per-event handlers ────────────────────────────────────────────────────

    async def _on_credit_analysis_completed(
        self, conn: asyncpg.Connection, p: dict, recorded_at: Any
    ) -> None:
        """Increment analyses_completed; update avg_confidence_score."""
        agent_id = p.get("session_id", "unknown")
        model_version = p.get("model_version", "unknown")
        decision = p.get("decision") or {}
        confidence = (
            decision.get("confidence")
            if isinstance(decision, dict)
            else getattr(decision, "confidence", None)
        ) or 0.0

        await conn.execute(
            """
            INSERT INTO agent_performance_ledger
                (agent_id, model_version, analyses_completed,
                 avg_confidence_score, first_seen_at, last_seen_at)
            VALUES ($1, $2, 1, $3, $4, $4)
            ON CONFLICT (agent_id, model_version) DO UPDATE SET
                analyses_completed   = agent_performance_ledger.analyses_completed + 1,
                avg_confidence_score = (
                    agent_performance_ledger.avg_confidence_score
                    * agent_performance_ledger.analyses_completed
                    + $3
                ) / (agent_performance_ledger.analyses_completed + 1),
                last_seen_at = GREATEST(agent_performance_ledger.last_seen_at, $4)
            """,
            agent_id,
            model_version,
            float(confidence),
            recorded_at,
        )

    async def _on_fraud_screening_completed(
        self, conn: asyncpg.Connection, p: dict, recorded_at: Any
    ) -> None:
        """Increment analyses_completed for fraud screening sessions."""
        agent_id = p.get("session_id", "unknown")
        model_version = p.get("screening_model_version", "unknown")
        await conn.execute(
            """
            INSERT INTO agent_performance_ledger
                (agent_id, model_version, analyses_completed,
                 first_seen_at, last_seen_at)
            VALUES ($1, $2, 1, $3, $3)
            ON CONFLICT (agent_id, model_version) DO UPDATE SET
                analyses_completed = agent_performance_ledger.analyses_completed + 1,
                last_seen_at = GREATEST(agent_performance_ledger.last_seen_at, $3)
            """,
            agent_id,
            model_version,
            recorded_at,
        )

    async def _on_decision_generated(
        self, conn: asyncpg.Connection, p: dict, recorded_at: Any
    ) -> None:
        """
        Increment decisions_generated and update approve/decline/refer rates.
        Rate = count_of_outcome / decisions_generated (recomputed incrementally).
        """
        agent_id = p.get("orchestrator_session_id", "unknown")
        model_versions = p.get("model_versions") or {}
        model_version = (
            next(iter(model_versions.values()), "unknown")
            if isinstance(model_versions, dict)
            else "unknown"
        )
        recommendation = (p.get("recommendation") or "").upper()

        approve_inc = 1 if recommendation == "APPROVE" else 0
        decline_inc = 1 if recommendation == "DECLINE" else 0
        refer_inc = 1 if recommendation == "REFER" else 0

        await conn.execute(
            """
            INSERT INTO agent_performance_ledger
                (agent_id, model_version, decisions_generated,
                 approve_rate, decline_rate, refer_rate,
                 first_seen_at, last_seen_at)
            VALUES ($1, $2, 1,
                    $3::numeric, $4::numeric, $5::numeric,
                    $6, $6)
            ON CONFLICT (agent_id, model_version) DO UPDATE SET
                decisions_generated = agent_performance_ledger.decisions_generated + 1,
                approve_rate = (
                    COALESCE(agent_performance_ledger.approve_rate, 0)
                    * agent_performance_ledger.decisions_generated + $3
                ) / (agent_performance_ledger.decisions_generated + 1),
                decline_rate = (
                    COALESCE(agent_performance_ledger.decline_rate, 0)
                    * agent_performance_ledger.decisions_generated + $4
                ) / (agent_performance_ledger.decisions_generated + 1),
                refer_rate = (
                    COALESCE(agent_performance_ledger.refer_rate, 0)
                    * agent_performance_ledger.decisions_generated + $5
                ) / (agent_performance_ledger.decisions_generated + 1),
                last_seen_at = GREATEST(agent_performance_ledger.last_seen_at, $6)
            """,
            agent_id,
            model_version,
            float(approve_inc),
            float(decline_inc),
            float(refer_inc),
            recorded_at,
        )

    async def _on_human_review_completed(
        self, conn: asyncpg.Connection, p: dict, recorded_at: Any
    ) -> None:
        """
        Track human overrides.
        override_rate = human_override_count / analyses_completed.
        """
        # Human review is linked to the originating orchestrator session,
        # but the event only carries application_id and reviewer_id.
        # We use application_id as agent_id key for the override tracking row.
        agent_id = p.get("application_id", "unknown")
        model_version = "human-review"
        override = bool(p.get("override", False))

        await conn.execute(
            """
            INSERT INTO agent_performance_ledger
                (agent_id, model_version, analyses_completed,
                 human_override_count, human_override_rate,
                 first_seen_at, last_seen_at)
            VALUES ($1, $2, 1, $3::int, $3::numeric, $4, $4)
            ON CONFLICT (agent_id, model_version) DO UPDATE SET
                analyses_completed  = agent_performance_ledger.analyses_completed + 1,
                human_override_count = agent_performance_ledger.human_override_count + $3::int,
                human_override_rate  = (
                    agent_performance_ledger.human_override_count + $3::int
                )::numeric
                / (agent_performance_ledger.analyses_completed + 1),
                last_seen_at = GREATEST(agent_performance_ledger.last_seen_at, $4)
            """,
            agent_id,
            model_version,
            1 if override else 0,
            recorded_at,
        )

    async def _on_agent_session_completed(
        self, conn: asyncpg.Connection, p: dict, recorded_at: Any
    ) -> None:
        """Update avg_duration_ms using incremental mean."""
        agent_id = p.get("session_id", "unknown")
        model_version = p.get("agent_type", "unknown")
        duration_ms = float(p.get("total_duration_ms") or 0)

        await conn.execute(
            """
            INSERT INTO agent_performance_ledger
                (agent_id, model_version, analyses_completed,
                 avg_duration_ms, first_seen_at, last_seen_at)
            VALUES ($1, $2, 1, $3, $4, $4)
            ON CONFLICT (agent_id, model_version) DO UPDATE SET
                analyses_completed = agent_performance_ledger.analyses_completed + 1,
                avg_duration_ms = (
                    COALESCE(agent_performance_ledger.avg_duration_ms, 0)
                    * agent_performance_ledger.analyses_completed + $3
                ) / (agent_performance_ledger.analyses_completed + 1),
                last_seen_at = GREATEST(agent_performance_ledger.last_seen_at, $4)
            """,
            agent_id,
            model_version,
            duration_ms,
            recorded_at,
        )

    # ── Rebuild ───────────────────────────────────────────────────────────────

    async def rebuild_from_scratch(self, pool: asyncpg.Pool) -> None:
        """
        Truncate the performance ledger table and reset checkpoint to 0.
        The daemon will replay all events from the beginning on next startup.
        """
        async with pool.acquire() as conn:
            await conn.execute("TRUNCATE agent_performance_ledger")
            await conn.execute(
                "UPDATE projection_checkpoints "
                "SET last_position = 0 "
                "WHERE projection_name = $1",
                self.name,
            )
