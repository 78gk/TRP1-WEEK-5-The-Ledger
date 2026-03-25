from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone

from ledger.event_store import EventStore
from ledger.schema.events import AuditIntegrityCheckRun


def _canonical_json(payload: dict) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _event_digest(payload: dict) -> str:
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _event_attr(event, name: str, default=None):
    if isinstance(event, dict):
        return event.get(name, default)
    return getattr(event, name, default)


def _event_payload(event) -> dict:
    payload = _event_attr(event, "payload", {})
    return payload if isinstance(payload, dict) else dict(payload)


@dataclass(slots=True)
class IntegrityCheckResult:
    """Typed result returned by run_integrity_check."""

    entity_type: str
    entity_id: str
    events_verified_count: int
    integrity_hash: str
    previous_hash: str | None
    chain_valid: bool
    tamper_detected: bool


async def run_integrity_check(
    store: EventStore,
    entity_type: str,
    entity_id: str,
) -> IntegrityCheckResult:
    """
    Verify append-only integrity for an entity stream and append an audit event.

    Chain model:
    - Primary stream: f"{entity_type}-{entity_id}"
    - Audit stream:   f"audit-{entity_type}-{entity_id}"
    - Each check stores the hash for the events it verified plus the prior hash.
    - If a previous check exists, the already-verified segment is re-hashed and
      compared against the previously recorded integrity hash to detect tampering.
    """

    primary_stream_id = f"{entity_type}-{entity_id}"
    audit_stream_id = f"audit-{entity_type}-{entity_id}"

    events = await store.load_stream(primary_stream_id)
    events = sorted(events, key=lambda event: _event_attr(event, "stream_position", -1))

    audit_events = await store.load_stream(audit_stream_id)
    prior_checks = [
        event
        for event in audit_events
        if _event_attr(event, "event_type") == "AuditIntegrityCheckRun"
    ]
    prior_checks.sort(key=lambda event: _event_attr(event, "stream_position", -1))

    latest_check = prior_checks[-1] if prior_checks else None
    latest_payload = _event_payload(latest_check) if latest_check else {}
    previous_hash = latest_payload.get("integrity_hash") if latest_check else None
    last_verified_position = int(latest_payload.get("last_verified_position", -1))

    tamper_detected = False
    if latest_check is not None:
        previously_verified = [
            event
            for event in events
            if _event_attr(event, "stream_position", -1) <= last_verified_position
        ]
        previous_segment_digests = "".join(
            _event_digest(_event_payload(event)) for event in previously_verified
        )

        hash_before_previous = None
        if len(prior_checks) > 1:
            hash_before_previous = _event_payload(prior_checks[-2]).get("integrity_hash")

        recomputed_previous_hash = hashlib.sha256(
            ((hash_before_previous or "") + previous_segment_digests).encode("utf-8")
        ).hexdigest()
        tamper_detected = recomputed_previous_hash != previous_hash

    events_to_verify = [
        event
        for event in events
        if _event_attr(event, "stream_position", -1) > last_verified_position
    ]
    new_segment_digests = "".join(
        _event_digest(_event_payload(event)) for event in events_to_verify
    )
    integrity_hash = hashlib.sha256(
        ((previous_hash or "") + new_segment_digests).encode("utf-8")
    ).hexdigest()

    check_event = AuditIntegrityCheckRun(
        entity_type=entity_type,
        entity_id=entity_id,
        check_timestamp=datetime.now(timezone.utc),
        events_verified_count=len(events),
        integrity_hash=integrity_hash,
        previous_hash=previous_hash,
        last_verified_position=_event_attr(events[-1], "stream_position", -1) if events else -1,
        chain_valid=not tamper_detected,
        tamper_detected=tamper_detected,
    )

    try:
        audit_version = await store.stream_version(audit_stream_id)
        await store.append(
            stream_id=audit_stream_id,
            events=[check_event.to_store_dict()],
            expected_version=audit_version,
        )
    except Exception:
        pass

    return IntegrityCheckResult(
        entity_type=entity_type,
        entity_id=entity_id,
        events_verified_count=len(events),
        integrity_hash=integrity_hash,
        previous_hash=previous_hash,
        chain_valid=not tamper_detected,
        tamper_detected=tamper_detected,
    )
