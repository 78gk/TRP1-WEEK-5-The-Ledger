"""
tests/conftest.py - shared fixtures
"""
import os
import random
import sys
from pathlib import Path

import asyncpg
from faker import Faker
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

random.seed(42)
Faker.seed(42)


@pytest.fixture
def db_url():
    return os.environ.get("TEST_DB_URL", "postgresql://localhost/apex_ledger_test")


@pytest.fixture
def sample_companies():
    from datagen.company_generator import generate_companies

    return generate_companies(10)


@pytest.fixture
def event_store_class():
    """Returns the EventStore class. Swap for real once implemented."""
    from ledger.event_store import EventStore

    return EventStore


@pytest.fixture
async def db_pool(db_url):
    """
    Shared asyncpg pool for tests that require a live PostgreSQL database.
    """
    try:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=5)
    except (OSError, asyncpg.InvalidCatalogNameError, asyncpg.PostgresError) as exc:
        pytest.skip(f"PostgreSQL not available: {exc}")

    try:
        yield pool
    finally:
        await pool.close()


@pytest.fixture
async def clean_db(db_pool):
    """
    Reset mutable event-store tables before each DB-backed test.
    """
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE TABLE outbox, projection_checkpoints, events, event_streams
            RESTART IDENTITY CASCADE
            """
        )

    yield


@pytest.fixture
async def event_store(db_pool, clean_db):
    """Real EventStore backed by the shared PostgreSQL pool."""
    from ledger.event_store import EventStore

    return EventStore(pool=db_pool)


@pytest.fixture
async def projection_daemon(event_store, db_pool):
    from ledger.projections.application_summary import (
        ApplicationSummaryProjection,
        CREATE_TABLE as APPLICATION_SUMMARY_DDL,
    )
    from ledger.projections.compliance_audit import (
        ComplianceAuditViewProjection,
        CREATE_TABLE as COMPLIANCE_AUDIT_DDL,
    )
    from ledger.projections.daemon import ProjectionDaemon

    async with db_pool.acquire() as conn:
        await conn.execute(APPLICATION_SUMMARY_DDL)
        await conn.execute(COMPLIANCE_AUDIT_DDL)
        await conn.execute("TRUNCATE TABLE application_summary, compliance_audit_view")

    yield ProjectionDaemon(
        event_store,
        db_pool,
        [ApplicationSummaryProjection(), ComplianceAuditViewProjection()],
    )


@pytest.fixture
async def compliance_projection(event_store, db_pool):
    from ledger.projections.compliance_audit import (
        ComplianceAuditViewProjection,
        CREATE_TABLE as COMPLIANCE_AUDIT_DDL,
    )

    async with db_pool.acquire() as conn:
        await conn.execute(COMPLIANCE_AUDIT_DDL)
        await conn.execute("TRUNCATE TABLE compliance_audit_view")

    return ComplianceAuditViewProjection()
