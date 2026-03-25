"""
tests/conftest.py - shared fixtures
"""
import asyncio
import contextlib
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

SCHEMA_SQL = (Path(__file__).parent.parent / "ledger" / "schema" / "schema.sql").read_text()


async def _ensure_base_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        exists = await conn.fetchval("SELECT to_regclass('public.events') IS NOT NULL")
        if exists:
            return
        await conn.execute('CREATE EXTENSION IF NOT EXISTS pgcrypto')
        await conn.execute(SCHEMA_SQL)


@pytest.fixture
def db_url():
    return os.environ.get("TEST_DB_URL", "postgresql://localhost/apex_ledger_test")


@pytest.fixture
def sample_companies():
    from datagen.company_generator import generate_companies

    return generate_companies(10)


@pytest.fixture
def event_store_class():
    from ledger.event_store import EventStore

    return EventStore


@pytest.fixture
async def db_pool(db_url):
    try:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=5)
    except (OSError, asyncpg.InvalidCatalogNameError, asyncpg.PostgresError) as exc:
        pytest.skip(f"PostgreSQL not available: {exc}")

    await _ensure_base_schema(pool)

    try:
        yield pool
    finally:
        await pool.close()


@pytest.fixture
async def clean_db(db_pool):
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
    from ledger.projections.agent_performance import (
        AgentPerformanceLedgerProjection,
        CREATE_TABLE as AGENT_PERFORMANCE_DDL,
    )
    from ledger.projections.daemon import ProjectionDaemon

    async with db_pool.acquire() as conn:
        await conn.execute(APPLICATION_SUMMARY_DDL)
        await conn.execute(COMPLIANCE_AUDIT_DDL)
        await conn.execute(AGENT_PERFORMANCE_DDL)
        await conn.execute("TRUNCATE TABLE application_summary, compliance_audit_view, agent_performance_ledger")

    yield ProjectionDaemon(
        event_store,
        db_pool,
        [
            ApplicationSummaryProjection(),
            ComplianceAuditViewProjection(),
            AgentPerformanceLedgerProjection(),
        ],
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


@pytest.fixture
async def mcp_server(event_store, db_pool, projection_daemon):
    from ledger.mcp.server import create_mcp_server

    daemon_task = asyncio.create_task(projection_daemon.run_forever(poll_interval_ms=50))
    server = create_mcp_server(event_store, db_pool, projection_daemon)

    try:
        yield server
    finally:
        await projection_daemon.stop()
        daemon_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await daemon_task
