import asyncio
import json

import pytest


pytestmark = pytest.mark.asyncio


async def test_full_lifecycle_via_mcp_tools_only(mcp_server):
    """
    Drive a complete loan application lifecycle using only MCP tool calls.
    """
    session_result = await mcp_server.call_tool(
        "start_agent_session",
        {
            "agent_id": "credit-agent",
            "session_id": "lifecycle-test-session",
            "model_version": "v2.4.1",
            "context_source": "fresh",
        },
    )
    session = session_result.structured_content
    assert "error_type" not in session

    app_result = await mcp_server.call_tool(
        "submit_application",
        {
            "application_id": "lifecycle-test-001",
            "applicant_id": "applicant-lifecycle",
            "requested_amount_usd": 500000,
            "loan_purpose": "expansion",
        },
    )
    application = app_result.structured_content
    assert "stream_id" in application

    credit_result = await mcp_server.call_tool(
        "record_credit_analysis",
        {
            "application_id": "lifecycle-test-001",
            "agent_id": "credit-agent",
            "session_id": "lifecycle-test-session",
            "model_version": "v2.4.1",
            "confidence_score": 0.85,
            "risk_tier": "LOW",
            "recommended_limit_usd": 500000,
        },
    )
    credit = credit_result.structured_content
    assert "error_type" not in credit

    fraud_result = await mcp_server.call_tool(
        "record_fraud_screening",
        {
            "application_id": "lifecycle-test-001",
            "agent_id": "fraud-agent",
            "session_id": "lifecycle-test-session",
            "fraud_score": 0.15,
            "risk_level": "LOW",
        },
    )
    fraud = fraud_result.structured_content
    assert "error_type" not in fraud

    for rule_id in ["REG-001", "REG-002", "REG-003"]:
        comp_result = await mcp_server.call_tool(
            "record_compliance_check",
            {
                "application_id": "lifecycle-test-001",
                "rule_id": rule_id,
                "passed": True,
            },
        )
        compliance_step = comp_result.structured_content
        assert "error_type" not in compliance_step

    decision_result = await mcp_server.call_tool(
        "generate_decision",
        {
            "application_id": "lifecycle-test-001",
            "recommendation": "APPROVE",
            "confidence_score": 0.85,
        },
    )
    decision = decision_result.structured_content
    assert "error_type" not in decision

    review_result = await mcp_server.call_tool(
        "record_human_review",
        {
            "application_id": "lifecycle-test-001",
            "reviewer_id": "LO-Test-Reviewer",
            "override": False,
            "final_decision": "APPROVE",
        },
    )
    review = review_result.structured_content
    assert review.get("final_decision") == "APPROVE"

    await asyncio.sleep(0.5)

    compliance_result = await mcp_server.read_resource(
        "ledger://applications/lifecycle-test-001/compliance"
    )
    compliance_payload = json.loads(compliance_result.contents[0].content)
    assert compliance_payload is not None
    assert len(compliance_payload.get("events", [])) > 0

    event_types_in_compliance = [
        row.get("event_type") for row in compliance_payload.get("events", [])
    ]
    for required in [
        "ComplianceCheckRequested",
        "ComplianceRulePassed",
        "ComplianceCheckCompleted",
    ]:
        assert required in event_types_in_compliance
    assert event_types_in_compliance.count("ComplianceRulePassed") >= 3
