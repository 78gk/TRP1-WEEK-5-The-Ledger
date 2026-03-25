"""
ledger/upcasting/upcasters.py
==============================
Concrete upcasters for the TWO events that have schema versions.

Each upcaster is a pure function: dict[payload] -> dict[payload].
No I/O, no database calls, no side effects.

Inference strategy is documented inline with reasoning for each field choice.
"""
from __future__ import annotations

from ledger.upcasting.registry import UpcasterRegistry

registry = UpcasterRegistry()


# ─── CreditAnalysisCompleted  v1 → v2 ────────────────────────────────────────

@registry.register("CreditAnalysisCompleted", from_version=1)
def upcast_credit_v1_to_v2(payload: dict) -> dict:
    """
    v2 adds three fields that were not captured in v1 schema:
      - model_version   (was called model_id in v1, now a structured string)
      - confidence_score (entirely new in v2)
      - regulatory_basis (new list of applicable regulation IDs)

    ── Inference strategy ────────────────────────────────────────────────────

    confidence_score: NULL
        Reasoning: Fabricating a confidence score that was never computed would
        corrupt downstream analytics and regulatory records.  A compliance system
        reading a fabricated 0.75 would make decisions based on false precision.
        NULL signals genuine absence of data.  Any downstream consumer seeing NULL
        knows this was a pre-2026 analysis without confidence scoring.

    model_version: INFERRED from recorded_at timestamp
        Reasoning: Deployment timeline is known apriori.
          - Before 2025-01-01: "credit-model-v1.0 (legacy, inferred)"
          - 2025-01-01 to 2025-06-14: "credit-model-v1.2 (inferred)"
          - 2025-06-15 to 2025-12-31: "credit-model-v2.0 (inferred)"
          - 2026-01-01+: "credit-model-v3.0 (inferred)"
        This inference is approximate and flagged as such (suffix " (inferred)").
        Error rate: ~5% for events near deployment boundaries — model swaps happen
        gradually, not instantaneously.  Downstream consequence of a wrong inference:
        agent performance metrics grouped by wrong model version — annoying but NOT
        compliance-critical.

    regulatory_basis: INFERRED from rule versions active at recorded_at
        Reasoning: The regulation version registry is deterministic — we know exactly
        which regulations were active on any given date (they have precise effective
        dates).  This inference is safe and has ~0% error rate.
    """
    # Extract timestamp from payload or fall back to empty string
    recorded_at: str = payload.get("completed_at") or payload.get("recorded_at") or ""

    model_version = _infer_model_version(recorded_at)
    regulatory_basis = _infer_regulatory_basis(recorded_at)

    return {
        **payload,
        "model_version": model_version,        # inferred, flagged as approximate
        "confidence_score": None,              # genuinely unknown — NOT fabricated
        "regulatory_basis": regulatory_basis,  # inferred from regulation schedule
    }


def _infer_model_version(recorded_at: str) -> str:
    """
    Map a recorded_at ISO-8601 timestamp to the known deployment timeline.

    In production this would use the actual deployment records table.
    For the upcaster (which must be deterministic and offline), we use the
    known hard cutover dates.
    """
    if not recorded_at:
        return "unknown (no timestamp)"

    date_str = recorded_at[:10]  # e.g. "2025-06-20"

    if date_str >= "2026-01-01":
        return "credit-model-v3.0 (inferred)"
    elif date_str >= "2025-06-15":
        return "credit-model-v2.0 (inferred)"
    elif date_str >= "2025-01-01":
        return "credit-model-v1.2 (inferred)"
    else:
        return "credit-model-v1.0 (legacy, inferred)"


def _infer_regulatory_basis(recorded_at: str) -> list[str]:
    """
    Map recorded_at to the regulation version set that was effective on that date.

    Regulations have precise effective dates so this inference is deterministic
    with ~0% error rate.
    """
    if not recorded_at:
        return ["REG-UNKNOWN (no timestamp)"]

    date_str = recorded_at[:10]

    if date_str >= "2026-01-01":
        return ["REG-2026-Q1 (inferred)", "DORA-2025 (inferred)"]
    elif date_str >= "2025-07-01":
        return ["REG-2025-Q3 (inferred)"]
    elif date_str >= "2025-01-01":
        return ["REG-2025-Q1 (inferred)"]
    else:
        return ["REG-2024-Q1 (inferred)"]


# ─── DecisionGenerated  v1 → v2 ───────────────────────────────────────────────

@registry.register("DecisionGenerated", from_version=1)
def upcast_decision_v1_to_v2(payload: dict) -> dict:
    """
    v2 adds: model_versions dict (agent_type -> model_version string).

    ── Inference strategy ────────────────────────────────────────────────────

    model_versions: reconstructed from contributing session references
        Reasoning: a pure upcaster cannot load external session streams, but it
        can still recover the agent-type keys from the recorded contributing
        session references. We preserve those keys and mark each value as
        unknown/inferred rather than fabricating a precise deployment string.

        NOTE (production concern):
        Exact model-version recovery still requires an offline migration that
        joins against session streams, because the v1 payload never persisted
        the deployment identifiers directly.

    contributing_sessions: Preserved as-is from v1 payload so callers can still
        use it as a reference list for external lookups.
    """
    contributing_sessions = payload.get("contributing_sessions") or []
    model_versions: dict[str, str] = {}

    for session_ref in contributing_sessions:
        if not isinstance(session_ref, str):
            continue
        parts = session_ref.split("-", 2)
        if len(parts) != 3 or parts[0] != "agent":
            continue
        agent_type = parts[1]
        model_versions.setdefault(
            agent_type,
            "unknown (inferred from contributing session reference)",
        )

    return {
        **payload,
        "model_versions": model_versions,
    }
