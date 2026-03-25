from __future__ import annotations

from datetime import datetime

from ledger.projections.compliance_audit import ComplianceAuditViewProjection


def _row_to_dict(row):
    return dict(row) if row else {"error": "not_found"}


def _event_to_dict(event):
    if isinstance(event, dict):
        return dict(event)
    return {
        "event_id": str(event.event_id),
        "stream_id": event.stream_id,
        "stream_position": event.stream_position,
        "global_position": event.global_position,
        "recorded_at": event.recorded_at.isoformat(),
        "metadata": event.metadata,
        "event_type": event.event_type,
        "event_version": event.event_version,
        "payload": event.payload,
    }


def register_resources(mcp, pool, projection_daemon):
    store = projection_daemon._store
    compliance_projection = ComplianceAuditViewProjection()

    @mcp.resource("ledger://applications/{application_id}")
    async def get_application(application_id: str) -> dict:
        """Read from ApplicationSummary projection. NEVER replays stream."""
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM application_summary WHERE application_id = $1",
                application_id,
            )
            return _row_to_dict(row)

    @mcp.resource("ledger://applications/{application_id}/compliance")
    async def get_compliance(application_id: str, as_of: str | None = None) -> dict:
        """
        Read from ComplianceAuditView projection.
        Supports temporal query via as_of timestamp.
        """
        if as_of:
            timestamp = datetime.fromisoformat(as_of)
            history = await compliance_projection.get_compliance_at(pool, application_id, timestamp)
            return {
                "application_id": application_id,
                "as_of": as_of,
                "events": history,
            }

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM compliance_audit_view
                WHERE application_id = $1
                ORDER BY global_position ASC
                """,
                application_id,
            )
            return {
                "application_id": application_id,
                "events": [dict(row) for row in rows],
            }

    @mcp.resource("ledger://applications/{application_id}/audit-trail")
    async def get_audit_trail(application_id: str) -> dict:
        """
        JUSTIFIED EXCEPTION: reads directly from the AuditLedger stream.
        Reason: audit trail retrieval requires full event-by-event history rather than
        a projection summary, so direct stream access is the intended exception.
        """
        events = await store.load_stream(f"audit-loan-{application_id}")
        return {
            "application_id": application_id,
            "stream_id": f"audit-loan-{application_id}",
            "events": [_event_to_dict(event) for event in events],
        }

    @mcp.resource("ledger://agents/{agent_id}/performance")
    async def get_agent_performance(agent_id: str) -> dict:
        """Read from AgentPerformanceLedger projection."""
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM agent_performance_ledger WHERE agent_id = $1",
                agent_id,
            )
            if not rows:
                return {"error": "not_found"}
            return {
                "agent_id": agent_id,
                "models": [dict(row) for row in rows],
            }

    @mcp.resource("ledger://agents/{agent_id}/sessions/{session_id}")
    async def get_agent_session(agent_id: str, session_id: str) -> dict:
        """
        JUSTIFIED EXCEPTION: reads directly from the AgentSession stream.
        Reason: crash recovery and fine-grained session inspection require the full
        event history rather than a projection summary.
        """
        events = await store.load_stream(f"agent-{agent_id}-{session_id}")
        if not events:
            return {"error": "not_found"}
        return {
            "agent_id": agent_id,
            "session_id": session_id,
            "stream_id": f"agent-{agent_id}-{session_id}",
            "events": [_event_to_dict(event) for event in events],
        }

    @mcp.resource("ledger://ledger/health")
    async def get_health() -> dict:
        """Returns per-projection lag in milliseconds."""
        lags = await projection_daemon.get_all_lags()
        return {
            "status": "healthy" if all(value < 1000 for value in lags.values()) else "degraded",
            "projections": {
                name: {"lag_ms": lag}
                for name, lag in lags.items()
            },
        }

    return None
