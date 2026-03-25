import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from ledger.integrity.audit_chain import run_integrity_check


pytestmark = pytest.mark.asyncio


async def test_upcaster_does_not_modify_stored_events(event_store, db_pool):
    event_id = uuid4()
    stream_id = "loan-test-upcast"
    v1_payload = {
        "application_id": "test-app-1",
        "session_id": "session-1",
        "decision": {
            "risk_tier": "MEDIUM",
            "recommended_limit_usd": 500000,
            "confidence": 0.82,
            "rationale": "Stable cash flow.",
            "key_concerns": [],
            "data_quality_caveats": [],
            "policy_overrides_applied": [],
        },
        "model_deployment_id": "deploy-1",
        "input_data_hash": "hash-1",
        "analysis_duration_ms": 1250,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO event_streams (stream_id, aggregate_type, current_version)
            VALUES ($1, $2, 0)
            """,
            stream_id,
            "loan",
        )
        await conn.execute(
            """
            INSERT INTO events (
                event_id,
                stream_id,
                stream_position,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES ($1, $2, 0, 'CreditAnalysisCompleted', 1, $3::jsonb, '{}'::jsonb)
            """,
            event_id,
            stream_id,
            json.dumps(v1_payload),
        )

    events = await event_store.load_stream(stream_id)
    assert len(events) == 1

    loaded = events[0]
    assert loaded.event_version == 2, f"Expected v2, got v{loaded.event_version}"
    assert "model_version" in loaded.payload
    assert loaded.payload["confidence_score"] is None

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT payload, event_version FROM events WHERE event_id = $1",
            event_id,
        )

    raw_payload = row["payload"]
    if isinstance(raw_payload, str):
        raw_payload = json.loads(raw_payload)

    assert row["event_version"] == 1, "Raw event_version must still be 1"
    assert "model_version" not in raw_payload
    assert "confidence_score" not in raw_payload
    assert "regulatory_basis" not in raw_payload


async def test_tamper_detection_after_db_modification(event_store, db_pool):
    entity_type = "loan"
    entity_id = "test-tamper-1"
    stream_id = f"{entity_type}-{entity_id}"

    events = [
        {
            "event_type": "ApplicationSubmitted",
            "event_version": 1,
            "payload": {
                "application_id": entity_id,
                "step": "submitted",
                "ordinal": 1,
            },
        },
        {
            "event_type": "CreditAnalysisRequested",
            "event_version": 1,
            "payload": {
                "application_id": entity_id,
                "step": "analysis_requested",
                "ordinal": 2,
            },
        },
        {
            "event_type": "DecisionRequested",
            "event_version": 1,
            "payload": {
                "application_id": entity_id,
                "step": "decision_requested",
                "ordinal": 3,
            },
        },
    ]

    await event_store.append(stream_id=stream_id, events=events, expected_version=-1)

    result1 = await run_integrity_check(event_store, entity_type, entity_id)
    assert result1.chain_valid is True
    assert result1.tamper_detected is False

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE events
            SET payload = '{"tampered": true}'::jsonb
            WHERE stream_id = $1 AND stream_position = 1
            """,
            stream_id,
        )

    result2 = await run_integrity_check(event_store, entity_type, entity_id)
    assert result2.tamper_detected is True


async def test_decision_upcaster_reconstructs_model_versions(event_store, db_pool):
    application_id = "test-decision-upcast"
    session_id = "decision-upcast-session"
    decision_event_id = uuid4()

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO event_streams (stream_id, aggregate_type, current_version)
            VALUES ($1, $2, 0)
            ON CONFLICT (stream_id) DO NOTHING
            """,
            f"agent-credit_analysis-{session_id}",
            "agent",
        )
        await conn.execute(
            """
            INSERT INTO events (
                event_id, stream_id, stream_position, event_type, event_version, payload, metadata
            )
            VALUES (
                gen_random_uuid(), $1, 0, 'AgentSessionStarted', 1, $2::jsonb, '{}'::jsonb
            )
            """,
            f"agent-credit_analysis-{session_id}",
            json.dumps(
                {
                    "session_id": session_id,
                    "agent_type": "credit_analysis",
                    "agent_id": "credit-agent",
                    "application_id": application_id,
                    "model_version": "credit-model-v9.9",
                    "langgraph_graph_version": "graph-v1",
                    "context_source": "fresh",
                    "context_token_count": 0,
                    "started_at": datetime.now(timezone.utc).isoformat(),
                }
            ),
        )

        await conn.execute(
            """
            INSERT INTO event_streams (stream_id, aggregate_type, current_version)
            VALUES ($1, $2, 0)
            """,
            f"loan-{application_id}",
            "loan",
        )
        await conn.execute(
            """
            INSERT INTO events (
                event_id, stream_id, stream_position, event_type, event_version, payload, metadata
            )
            VALUES ($1, $2, 0, 'DecisionGenerated', 1, $3::jsonb, '{}'::jsonb)
            """,
            decision_event_id,
            f"loan-{application_id}",
            json.dumps(
                {
                    "application_id": application_id,
                    "orchestrator_session_id": "orchestrator-1",
                    "recommendation": "APPROVE",
                    "confidence": 0.91,
                    "approved_amount_usd": 100000,
                    "conditions": [],
                    "executive_summary": "legacy decision",
                    "key_risks": [],
                    "contributing_sessions": [f"agent-credit_analysis-{session_id}"],
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                }
            ),
        )

    events = await event_store.load_stream(f"loan-{application_id}")
    assert len(events) == 1
    loaded = events[0]
    assert loaded.event_version == 2
    assert loaded.payload["model_versions"]["credit_analysis"] == "credit-model-v9.9"

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT payload, event_version FROM events WHERE event_id = $1",
            decision_event_id,
        )

    raw_payload = row["payload"]
    if isinstance(raw_payload, str):
        raw_payload = json.loads(raw_payload)

    assert row["event_version"] == 1
    assert "model_versions" not in raw_payload
