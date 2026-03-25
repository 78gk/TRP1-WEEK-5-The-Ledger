"""
Integrity and tamper-detection utilities.
"""

from ledger.integrity.audit_chain import IntegrityCheckResult, run_integrity_check

__all__ = ["IntegrityCheckResult", "run_integrity_check"]
