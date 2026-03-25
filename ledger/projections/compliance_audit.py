"""
ledger/projections/compliance_audit.py
=======================================
Append-style projection: one row per compliance rule check event.
Designed for temporal querying — "what was the compliance state at time T?"

Registered with ProjectionDaemon — never called synchronously.
"""
from __future__ import annotations

import json
from typing import Any

import asyncpg


# ── DDL ───────────────────────────────────────────────────────────────────────

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS compliance_audit_view (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    application_id    TEXT        NOT NULL,
    event_type        TEXT        NOT NULL,
    rule_id           TEXT,
    rule_version      TEXT,
    verdict           TEXT,
    details           JSONB,
    event_timestamp   TIMESTAMPTZ,
    global_position   BIGINT,
    UNIQUE (application_id, event_type, rule_id, global_position)
);
CREATE INDEX IF NOT EXISTS idx_cav_app_time
    ON compliance_audit_view (application_id, event_timestamp);
"""


# ── Projection class ──────────────────────────────────────────────────────────

class ComplianceAuditViewProjection:
    """
    Append-only compliance audit log, one row per rule evaluation.

    - Every compliance event is inserted with ``ON CONFLICT DO NOTHING``
      so replays are idempotent.
    - ``get_compliance_at()`` performs a pure temporal read-query without
      touching the write side.
    - ``rebuild_from_scratch()`` truncates and resets the checkpoint so the
      daemon can replay the full history.
    """

    name = "compliance_audit_view"
    subscribed_events = [
        "ComplianceCheckRequested",
        "ComplianceCheckInitiated",
        "ComplianceRulePassed",
        "ComplianceRuleFailed",
        "ComplianceRuleNoted",
        "ComplianceCheckCompleted",
    ]

    # ── Event handler ─────────────────────────────────────────────────────────

    async def handle(self, event: Any, pool: asyncpg.Pool) -> None:
        et = event["event_type"] if isinstance(event, dict) else event.event_type
        p = event["payload"] if isinstance(event, dict) else event.payload
        recorded_at = (
            event.get("recorded_at") if isinstance(event, dict) else event.recorded_at
        )
        gp = (
            event.get("global_position", 0)
            if isinstance(event, dict)
            else event.global_position
        )

        verdict = self._derive_verdict(et, p)

        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO compliance_audit_view
                    (application_id, event_type, rule_id, rule_version,
                     verdict, details, event_timestamp, global_position)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
                ON CONFLICT DO NOTHING
                """,
                p.get("application_id"),
                et,
                p.get("rule_id"),
                p.get("rule_version"),
                verdict,
                json.dumps(p),
                recorded_at,
                gp,
            )

    # ── Temporal query ────────────────────────────────────────────────────────

    async def get_compliance_at(
        self,
        pool: asyncpg.Pool,
        application_id: str,
        timestamp: Any,
    ) -> list[dict]:
        """
        Temporal query: return all compliance events for *application_id*
        that were recorded at or before *timestamp*, in stream order.

        This is a pure read — it never mutates any table.
        """
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT *
                FROM compliance_audit_view
                WHERE application_id = $1
                  AND event_timestamp <= $2
                ORDER BY global_position ASC
                """,
                application_id,
                timestamp,
            )
        return [dict(row) for row in rows]

    # ── Rebuild ───────────────────────────────────────────────────────────────

    async def rebuild_from_scratch(self, pool: asyncpg.Pool) -> None:
        """
        Prepare a shadow table for replay and atomically swap it into place.

        This keeps a readable relation named ``compliance_audit_view`` present
        throughout the rebuild process rather than truncating the live table.
        """
        async with pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS compliance_audit_view_rebuild
                (LIKE compliance_audit_view INCLUDING ALL)
                """
            )
            await conn.execute("TRUNCATE compliance_audit_view_rebuild")
            await conn.execute(
                "UPDATE projection_checkpoints "
                "SET last_position = 0 "
                "WHERE projection_name = $1",
                self.name,
            )
            await conn.execute(
                """
                DO $$
                BEGIN
                    IF to_regclass('public.compliance_audit_view_old') IS NOT NULL THEN
                        DROP TABLE compliance_audit_view_old;
                    END IF;
                END $$;
                """
            )
            async with conn.transaction():
                await conn.execute(
                    "ALTER TABLE compliance_audit_view RENAME TO compliance_audit_view_old"
                )
                await conn.execute(
                    "ALTER TABLE compliance_audit_view_rebuild RENAME TO compliance_audit_view"
                )
                await conn.execute(
                    """
                    CREATE TABLE compliance_audit_view_rebuild
                    (LIKE compliance_audit_view INCLUDING ALL)
                    """
                )
                await conn.execute("DROP TABLE compliance_audit_view_old")

    # ── Internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _derive_verdict(event_type: str, payload: dict) -> str | None:
        """
        Map event type to a short verdict string for quick filtering.
        Returns None for events that don't represent a rule verdict.
        """
        mapping = {
            "ComplianceRulePassed": "PASSED",
            "ComplianceRuleFailed": "FAILED",
            "ComplianceRuleNoted": "NOTED",
            "ComplianceCheckCompleted": payload.get("overall_verdict"),
            "ComplianceCheckInitiated": "INITIATED",
            "ComplianceCheckRequested": "REQUESTED",
        }
        return mapping.get(event_type)
