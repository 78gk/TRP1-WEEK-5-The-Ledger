

TRP1  ·  Arc 5: Integration & Protocol Architecture
TRP1 WEEK 5: The Ledger
## Agentic Event Store & Enterprise Audit Infrastructure.
Building the immutable memory and governance backbone for multi-agent AI systems at
production scale.
If 2025 was the year of the agent, 2026 is the year multi-agent systems
move into production. The shift depends on one thing: infrastructure that
can be trusted. An event store is not optional infrastructure for production
AI. It is the foundation.
Builds on:
## Week 1 Governance Hooks & Intent Traceability
## Week 2 Automaton Auditor
## Week 4 Brownfield Cartographer

## Why This Project
Every system you have built in this program has a memory problem. The Cartographer's
lineage graph is rebuilt from scratch on each run. The Automaton Auditor's judgements are
lost when the process ends. Week 1's governance hooks produce an intent log that no other
system reads. These are not bugs — they are the natural limitations of systems that have no
shared, persistent, append-only memory.
The Ledger fixes this permanently. It is the event store that all other systems in this program
should have been writing to from Week 1. By the end of this week, you will have a
production-quality event sourcing infrastructure that: makes agent decisions auditable and
reproducible, enables temporal queries for compliance and debugging, provides the append-
only ledger that prevents the ephemeral memory failure mode described in the Gas Town
pattern, and exposes everything as a typed, queryable API that downstream systems can
consume.
The business case is precise. In 2026, the number-one reason enterprise AI deployments
fail to reach production is not model quality — it is governance and auditability. Regulators,
auditors, and enterprise risk teams require an immutable record of every AI decision and the
data that informed it. The Ledger is that record. An FDE who can deploy it in the first week of
a client engagement immediately unblocks the governance conversation that is otherwise
the last thing to get resolved.

## The Compounding Connection
This project is the retroactive foundation for the entire program. When you build the Ledger, you
are building the infrastructure that all prior projects should have been using. Your Week 2 audit
verdicts become events in a GovernanceJudgement stream. The Ledger does not add a new
system — it connects the ones you already have.

## New Skills This Week
## Technical Skills
● Event store schema design: Append-only tables, stream partitioning, hot/cold
storage, PostgreSQL LISTEN/NOTIFY for real-time subscriptions

TRP1  ·  Arc 5: Integration & Protocol Architecture
● CQRS implementation: Separating command handlers from query handlers,
projection management, eventual consistency patterns
● Aggregate design: Consistency boundaries, business rule enforcement in domain
logic, state machine patterns
● Optimistic concurrency control: Version-based conflict detection, retry strategies,
conflict resolution patterns
● Async projection daemon: Checkpoint management, fault-tolerant background
processing, projection lag measurement
● Upcasting & schema evolution: Version migration chains, inference vs. null
strategies, immutability guarantees
● Cryptographic audit chains: Hash chain construction, tamper detection, regulatory
package generation
● MCP command/resource architecture: Tool design for LLM consumers, structured
error types, precondition documentation

FDE Skills
● The governance conversation: Ability to translate "we need auditability" from a
risk/compliance stakeholder into a specific event store deployment recommendation
within 48 hours
● Enterprise stack translation: Mapping your PostgreSQL implementation to
Marten/Wolverine (.NET) and EventStoreDB for clients who already have a stack
preference
● The one-way door conversation: Knowing how to communicate the migration
complexity and long-term commitment of adopting event sourcing, so clients make
the decision with accurate information
● SLO-based architecture: Designing systems to explicit performance contracts
rather than "as fast as possible" — the foundation of production-grade FDE work

## The Week Standard
By end of this week, you must be able to demonstrate: "Show me the complete decision history
of application ID X" — from first event to final decision, with every AI agent action, every
compliance check, every human review, all causal links intact, temporal query to any point in the
lifecycle, and cryptographic integrity verification. If you cannot run this demonstration in under 60
seconds, the week is not complete.

## Reading Material
● An empirical characterization of event sourced systems and their schema evolution —
Lessons from industry

TRP1  ·  Arc 5: Integration & Protocol Architecture

Phase 0 — Domain Reconnaissance (Day 1, Morning)
Event sourcing is one of the most misunderstood patterns in enterprise software. Most
engineers who say they have used it have used a version of it — usually without optimistic
concurrency control, without projection management, without upcasting, and without
understanding why any of those things matter. Phase 0 establishes the conceptual precision
required to build the Ledger correctly.
## Core Concepts — Required Mastery
Event Sourcing vs. Event-Driven Architecture
These are not the same thing. Event-Driven Architecture (EDA) uses events as messages
between services — the sender fires and forgets. Event Sourcing uses events as the system's
source of truth — the events ARE the database. Your system today (agent activity tracing
component callbacks, the Automaton Auditor's verdict stream) is EDA. The Ledger is event
sourcing. The distinction matters because EDA events can be dropped or lost; event store
entries never can. Study: Confluent's "Future of AI Agents is Event-Driven" (2025) and contrast
with Greg Young's "CQRS and Event Sourcing" talks.
## Aggregate Boundaries & Domain Events
An aggregate is a consistency boundary — a cluster of domain objects that must be mutated
atomically. An event is the record of a fact that happened to an aggregate. The critical rule:
aggregates communicate only through events, never through direct method calls. In the AI era,
each AI agent is a natural aggregate boundary: its decisions are facts, recorded as events, never
mutated. Study: Vernon's "Implementing Domain-Driven Design", Chapter 10 on aggregates.
CQRS — Command Query Responsibility Segregation
Write operations (Commands) and read operations (Queries) are handled by separate models.
Commands append events to streams. Queries read from projections built from those events.
The separation enables: independent scaling of reads and writes, multiple read-optimised
projections from the same event stream, and the ability to rebuild any read model by replaying
events. In the MCP context: MCP Tools are Commands; MCP Resources are Queries against
projections.
## Optimistic Concurrency Control
In an event store, two processes can simultaneously try to append to the same stream. Without
concurrency control, you get split-brain state. The solution: every append operation specifies an
expected_version — the stream version it read before making its decision. If the stream's actual
version has advanced (because someone else appended), the operation is rejected with a
concurrency exception. The caller must reload and retry. This is how the Ledger prevents two AI
agents from simultaneously making conflicting decisions. No locks required. No transactions
spanning multiple aggregates.

TRP1  ·  Arc 5: Integration & Protocol Architecture
Projections — Inline vs. Async
A projection transforms events into a read model. Inline projections update synchronously in the
same transaction as the event write — strong consistency, higher write latency. Async
projections update asynchronously via a background daemon — lower write latency, eventual
consistency, and the ability to rebuild from scratch by replaying. The Marten library (the
enterprise .NET standard for PostgreSQL-backed event stores) calls its async projection runner
the "Async Daemon." Python equivalents achieve the same pattern with background asyncio
tasks. Study: Marten docs on projection lifecycle; EventStoreDB catch-up subscriptions.
## Upcasting — Handling Schema Evolution
In a CRUD system, you run a migration and the data changes. In an event store, the past is
immutable. When your event schema evolves, you write an upcaster — a function that transforms
old event structures into new ones at read time, without touching the stored events. This is the
event sourcing solution to the problem identified by schema evolution analysis tools. In
production, upcasters are registered in a chain: v1→v2→v3, applied automatically whenever old
events are loaded. An event store without upcasting is an event store that will eventually break
under the weight of its own history.
## The Outbox Pattern — Guaranteed Event Delivery
The classic distributed systems problem: you append an event to the store AND need to publish
it to a message bus (Kafka, Redis Streams, RabbitMQ). If the store write succeeds but the
publish fails, your read models and downstream systems are inconsistent. The Outbox Pattern
solves this: write events to both the event store and an "outbox" table in the same database
transaction. A separate process polls the outbox and publishes reliably. This is how you connect
The Ledger to the Polyglot Bridge (Week 10).
## The Gas Town Persistent Ledger Pattern
Named for the infrastructure pattern in agentic systems where agent context is lost on process
restart. The solution: every agent action is written to the event store as an event before the action
is executed. On restart, the agent replays its event stream to reconstruct its context window. This
is not just logging — it is the agent's memory backed by the most reliable storage primitive
available: an append-only, ACID-compliant, PostgreSQL-backed event stream.


Stack Orientation — Enterprise Tools in 2026
The enterprise market has converged on two primary event store backends. You must
understand both even if you implement only one.

## TOOL STACK BEST FOR ENTERPRISE
## ADOPTION
## YOUR CHOICE IN THIS
## CHALLENGE
PostgreSQL +
psycopg3
## Python
## (primary)
## Single-database
architectures; teams
already on Postgres;
FDE rapid deployment
Extremely high —
Postgres is
everywhere
PRIMARY — Build the event store
schema and all phases using
Postgres + asyncpg/psycopg3
EventStoreDB
## 24.x
Any (HTTP
## API)
Dedicated high-
throughput event
stores; persistent
subscriptions at scale;
native gRPC streaming
Growing — the
purpose-built
standard
REFERENCE — Know the API;
document in DOMAIN_NOTES how
your Postgres schema maps to
EventStoreDB concepts

TRP1  ·  Arc 5: Integration & Protocol Architecture
## TOOL STACK BEST FOR ENTERPRISE
## ADOPTION
## YOUR CHOICE IN THIS
## CHALLENGE
## Marten 7.x +
## Wolverine
.NET / C# Enterprise .NET shops;
Async Daemon for
projection
management;
Wolverine for command
routing
Dominant in .NET
enterprise
CONCEPTUAL — Study the
architecture; your Python
implementation should mirror the
same patterns
## Kafka + Kafka
## Streams
## Any Very-high-throughput
event streaming; not a
true event store
(retention limits)
Ubiquitous in
large enterprise
INTEGRATION — Week 10 connects
The Ledger to Kafka via the Outbox
pattern
Redis Streams Any Lower-latency pub/sub;
projection fan-out; not
durable by default
Common as event
bus layer
INTEGRATION — Use Redis
Streams for real-time projection
update notifications

DOMAIN_NOTES.md — Graded Deliverable
Produce a DOMAIN_NOTES.md before writing any implementation code. It must answer all
of the following with specificity, not generality. This document is assessed independently of
your code — a candidate who writes excellent code but cannot reason about the tradeoffs is
not ready for enterprise event sourcing work.
- EDA vs. ES distinction: A component uses callbacks (like LangChain traces) to
capture event-like data. Is this Event-Driven Architecture (EDA) or Event Sourcing
(ES)? If you redesigned it using The Ledger, what exactly would change in the
architecture and what would you gain?
- The aggregate question: In the scenario below, you will build four aggregates.
Identify one alternative boundary you considered and rejected. What coupling
problem does your chosen boundary prevent?
- Concurrency in practice: Two AI agents simultaneously process the same loan
application and both call append_events with expected_version=3. Trace the exact
sequence of operations in your event store. What does the losing agent receive, and
what must it do next?
- Projection lag and its consequences: Your LoanApplication projection is
eventually consistent with a typical lag of 200ms. A loan officer queries "available
credit limit" immediately after an agent commits a disbursement event. They see the
old limit. What does your system do, and how do you communicate this to the user
interface?
- The upcasting scenario: The CreditDecisionMade event was defined in 2024 with
{application_id, decision, reason}. In 2026 it needs {application_id, decision, reason,
model_version, confidence_score, regulatory_basis}. Write the upcaster. What is
your inference strategy for historical events that predate model_version?
- The Marten Async Daemon parallel: Marten 7.0 introduced distributed projection
execution across multiple nodes. Describe how you would achieve the same pattern
in your Python implementation. What coordination primitive do you use, and what
failure mode does it guard against?

## The Scenario — Apex Financial Services
Apex Financial Services is deploying a multi-agent AI platform to process commercial loan
applications. Four specialized AI agents collaborate on each application: a CreditAnalysis
agent evaluates financial risk, a FraudDetection agent screens for anomalous patterns, a

TRP1  ·  Arc 5: Integration & Protocol Architecture
ComplianceAgent verifies regulatory eligibility, and a DecisionOrchestrator synthesises their
outputs and produces a final recommendation. Human loan officers review the
recommendation and make the final binding decision.
The regulatory environment requires: a complete, immutable audit trail of every AI decision
and the data that informed it; the ability to reconstruct the exact state of any application at
any point in time for regulatory examination; temporal queries (e.g., "what would the credit
decision have been if we had used last month's risk model?"); and cryptographic integrity —
any tampering with the audit trail must be detectable. The CTO has mandated that the
system must not be modified to add auditability after the fact — auditability must be the
architecture, not an annotation.
This is the canonical environment where event sourcing is not just beneficial — it is the only
architecture that satisfies the requirements. Your task is to build The Ledger: the event store
and its surrounding infrastructure that makes this system governable.

## Why This Scenario
Financial services is the highest-density event sourcing environment in enterprise software.
Every loan decision, every risk calculation, every compliance check is a regulated event. The
same architecture applies directly to any domain where audit trails are non-negotiable:
healthcare prior authorisations, government benefit decisions, insurance claim adjudication, and
— directly relevant to your work — AI agent decision logs in any enterprise deployment. Master
this scenario and you have mastered the pattern for all of them.

## The Four Aggregates
## AGGREGATE STREAM ID
## FORMAT
## WHAT IT TRACKS KEY BUSINESS INVARIANTS
LoanApplication loan-
## {application_id}
Full lifecycle of a
commercial loan
application from
submission to decision
Cannot transition from Approved to
UnderReview; cannot be approved if
compliance check is pending; credit
limit cannot exceed agent-assessed
maximum
AgentSession agent-{agent_id}-
## {session_id}
All actions taken by a
specific AI agent instance
during a work session,
including model version,
input data hashes,
reasoning trace, and
outputs
Every output event must reference a
ContextLoaded event; every decision
must reference the specific model
version that produced it
ComplianceReco
rd
compliance-
## {application_id}
Regulatory checks, rule
evaluations, and
compliance verdicts for
each application
Cannot issue a compliance clearance
without all mandatory checks; every
check must reference the specific
regulation version evaluated against
AuditLedger audit-
## {entity_type}-
## {entity_id}
Cross-cutting audit trail
linking events across all
aggregates for a single
business entity
Append-only; no events may be
removed; must maintain cross-stream
causal ordering via correlation_id
chains

## The Event Catalogue
These are the events you will implement. The catalogue is intentionally incomplete —
identifying the missing events is part of the Phase 1 domain exercise.

TRP1  ·  Arc 5: Integration & Protocol Architecture
## EVENT TYPE AGGREGAT
## E
## VERSI
## ON
## KEY PAYLOAD FIELDS
ApplicationSubmitte
d
LoanApplicati
on
1 application_id, applicant_id, requested_amount_usd,
loan_purpose, submission_channel, submitted_at
CreditAnalysisRequ
ested
LoanApplicati
on
1 application_id, assigned_agent_id, requested_at, priority
CreditAnalysisCom
pleted
AgentSession 2 application_id, agent_id, session_id, model_version,
confidence_score, risk_tier, recommended_limit_usd,
analysis_duration_ms, input_data_hash
FraudScreeningCo
mpleted
AgentSession 1 application_id, agent_id, fraud_score, anomaly_flags[],
screening_model_version, input_data_hash
ComplianceCheckR
equested
ComplianceR
ecord
1 application_id, regulation_set_version, checks_required[]
ComplianceRulePa
ssed
ComplianceR
ecord
1 application_id, rule_id, rule_version, evaluation_timestamp,
evidence_hash
ComplianceRuleFai
led
ComplianceR
ecord
1 application_id, rule_id, rule_version, failure_reason,
remediation_required
DecisionGenerated LoanApplicati
on
2 application_id, orchestrator_agent_id, recommendation
(APPROVE/DECLINE/REFER), confidence_score,
contributing_agent_sessions[], decision_basis_summary,
model_versions{}
HumanReviewCom
pleted
LoanApplicati
on
1 application_id, reviewer_id, override (bool), final_decision,
override_reason (if override)
ApplicationApprove
d
LoanApplicati
on
1 application_id, approved_amount_usd, interest_rate,
conditions[], approved_by (human_id or "auto"),
effective_date
ApplicationDeclined LoanApplicati
on
1 application_id, decline_reasons[], declined_by,
adverse_action_notice_required (bool)
AgentContextLoade
d
AgentSession 1 agent_id, session_id, context_source,
event_replay_from_position, context_token_count,
model_version
AuditIntegrityCheck
## Run
AuditLedger 1 entity_id, check_timestamp, events_verified_count,
integrity_hash, previous_hash (chain)

PHASE 1  ·  The Event Store Core — PostgreSQL Schema & Interface
Build the event store foundation. Everything else is built on this. The schema is not a
suggestion — it is the contract that every other component in this program will eventually
write to and read from. Please identify and report if there are missing elements that could
improve the schema validity in future scenarios.
## Database Schema
Create the following tables. Justify every column in DESIGN.md — columns you cannot
justify should not exist.

CREATE TABLE events (
event_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
stream_id        TEXT NOT NULL,

TRP1  ·  Arc 5: Integration & Protocol Architecture
stream_position  BIGINT NOT NULL,
global_position  BIGINT GENERATED ALWAYS AS IDENTITY,
event_type       TEXT NOT NULL,
event_version    SMALLINT NOT NULL DEFAULT 1,
payload          JSONB NOT NULL,
metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
recorded_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)
## );

CREATE INDEX idx_events_stream_id ON events (stream_id, stream_position);
CREATE INDEX idx_events_global_pos ON events (global_position);
CREATE INDEX idx_events_type ON events (event_type);
CREATE INDEX idx_events_recorded ON events (recorded_at);

CREATE TABLE event_streams (
stream_id        TEXT PRIMARY KEY,
aggregate_type   TEXT NOT NULL,
current_version  BIGINT NOT NULL DEFAULT 0,
created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
archived_at      TIMESTAMPTZ,
metadata         JSONB NOT NULL DEFAULT '{}'::jsonb
## );

CREATE TABLE projection_checkpoints (
projection_name  TEXT PRIMARY KEY,
last_position    BIGINT NOT NULL DEFAULT 0,
updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
## );

CREATE TABLE outbox (
id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
event_id         UUID NOT NULL REFERENCES events(event_id),
destination      TEXT NOT NULL,
payload          JSONB NOT NULL,
created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
published_at     TIMESTAMPTZ,
attempts         SMALLINT NOT NULL DEFAULT 0
## );

## Core Python Interface
Implement EventStore as an async Python class. The interface is fixed — implementation is
yours.

class EventStore:
async def append(
self,
stream_id: str,
events: list[BaseEvent],
expected_version: int,          # -1 = new stream; N = exact version required
correlation_id: str | None = None,
causation_id:   str | None = None,
) -> int:                            # returns new stream version
## """
Atomically appends events to stream_id.
Raises OptimisticConcurrencyError if stream version != expected_version.
Writes to outbox in same transaction.
## """

async def load_stream(

TRP1  ·  Arc 5: Integration & Protocol Architecture
self,
stream_id: str,
from_position: int = 0,
to_position:   int | None = None,
) -> list[StoredEvent]:             # events in stream order, upcasted

async def load_all(
self,
from_global_position: int = 0,
event_types: list[str] | None = None,
batch_size: int = 500,
) -> AsyncIterator[StoredEvent]:   # async generator, efficient for replay

async def stream_version(self, stream_id: str) -> int:
async def archive_stream(self, stream_id: str) -> None:
async def get_stream_metadata(self, stream_id: str) -> StreamMetadata:

Optimistic Concurrency — The Double-Decision Test
This is the most critical test in Phase 1. Two AI agents simultaneously attempt to append a
CreditAnalysisCompleted event to the same loan application stream. Both read the stream at
version 3 and pass expected_version=3 to their append call. Exactly one must succeed. The
other must receive OptimisticConcurrencyError and retry after reloading the stream.
Implement a test that spawns two concurrent asyncio tasks doing this. The test must assert:
(a) total events appended to the stream = 4 (not 5), (b) the winning task's event has
stream_position=4, (c) the losing task's OptimisticConcurrencyError is raised, not silently
swallowed.
## Why This Matters
In the Apex loan scenario, this test represents two fraud-detection agents simultaneously
flagging the same application. Without optimistic concurrency, both flags are applied and the
application's state becomes inconsistent — no one knows which fraud score is authoritative. With
it, one agent's decision wins; the other must reload and see whether its analysis is still relevant.
This is not an edge case — at 1,000 applications/hour with 4 agents each, concurrency collisions
happen constantly.

PHASE 2  ·  Domain Logic — Aggregates, Commands & Business Rules
Implement the domain logic for LoanApplication and AgentSession. The pattern: command
received → aggregate state reconstructed by replaying events → business rules validated →
new events appended.
## The Command Handler Pattern
# Every command handler follows this exact structure:
async def handle_credit_analysis_completed(
cmd: CreditAnalysisCompletedCommand,
store: EventStore,
## ) -> None:
# 1. Reconstruct current aggregate state from event history
app = await LoanApplicationAggregate.load(store, cmd.application_id)
agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)

# 2. Validate — all business rules checked BEFORE any state change
app.assert_awaiting_credit_analysis()
agent.assert_context_loaded()                    # Gas Town pattern
agent.assert_model_version_current(cmd.model_version)

TRP1  ·  Arc 5: Integration & Protocol Architecture

# 3. Determine new events — pure logic, no I/O
new_events = [
CreditAnalysisCompleted(
application_id = cmd.application_id,
agent_id       = cmd.agent_id,
session_id     = cmd.session_id,
model_version  = cmd.model_version,
confidence_score = cmd.confidence_score,
risk_tier      = cmd.risk_tier,
recommended_limit_usd = cmd.recommended_limit_usd,
analysis_duration_ms  = cmd.duration_ms,
input_data_hash = hash_inputs(cmd.input_data),
## )
## ]

# 4. Append atomically — optimistic concurrency enforced by store
await store.append(
stream_id        = f"loan-{cmd.application_id}",
events           = new_events,
expected_version = app.version,
correlation_id   = cmd.correlation_id,
causation_id     = cmd.causation_id,
## )

Business Rules to Enforce
The following rules must be enforced in the aggregate domain logic, not in the API layer. A
rule that is only checked in a request handler is not a business rule — it is a UI validation.
- Application state machine: Valid transitions only: Submitted → AwaitingAnalysis →
AnalysisComplete → ComplianceReview → PendingDecision →
ApprovedPendingHuman / DeclinedPendingHuman → FinalApproved / FinalDeclined.
Any out-of-order transition raises DomainError.
- Agent context requirement (Gas Town): An AgentSession aggregate MUST have
an AgentContextLoaded event as its first event before any decision event can be
appended. This enforces the persistent ledger pattern — no agent may make a
decision without first declaring its context source.
- Model version locking: Once a CreditAnalysisCompleted event is appended for an
application, no further CreditAnalysisCompleted events may be appended for the
same application unless the first was superseded by a HumanReviewOverride. This
prevents analysis churn.
- Confidence floor: A DecisionGenerated event with confidence_score < 0.6 must set
recommendation = "REFER" regardless of the orchestrator's analysis. This is a
regulatory requirement, enforced in the aggregate.
- Compliance dependency: An ApplicationApproved event cannot be appended
unless all ComplianceRulePassed events for the application's required checks are
present in the ComplianceRecord stream. The LoanApplication aggregate must hold
a reference to check this.
- Causal chain enforcement: Every DecisionGenerated event's
contributing_agent_sessions[] list must reference only AgentSession stream IDs that
contain a decision event for this application_id. An orchestrator that references
sessions that never processed this application must be rejected.

## Aggregate State Reconstruction

TRP1  ·  Arc 5: Integration & Protocol Architecture
Each aggregate must implement a load() classmethod that replays its event stream and
applies each event to build current state. The apply pattern must be explicit — one method
per event type:
class LoanApplicationAggregate:
## @classmethod
async def load(cls, store: EventStore, application_id: str) -> "LoanApplicationAggregate":
events = await store.load_stream(f"loan-{application_id}")
agg = cls(application_id=application_id)
for event in events:
agg._apply(event)
return agg

def _apply(self, event: StoredEvent) -> None:
handler = getattr(self, f"_on_{event.event_type}", None)
if handler:
handler(event)
self.version = event.stream_position

def _on_ApplicationSubmitted(self, event: StoredEvent) -> None:
self.state = ApplicationState.SUBMITTED
self.applicant_id = event.payload["applicant_id"]
self.requested_amount = event.payload["requested_amount_usd"]

def _on_ApplicationApproved(self, event: StoredEvent) -> None:
self.state = ApplicationState.FINAL_APPROVED
self.approved_amount = event.payload["approved_amount_usd"]


PHASE 3  ·  Projections — CQRS Read Models & Async Daemon
Projections are the read side of CQRS. They subscribe to the event stream and maintain
read-optimised views that can be queried without loading and replaying aggregate streams.
Build three projections and the async daemon that keeps them current.
## The Async Projection Daemon
The daemon is a background asyncio task that continuously polls the events table from the
last processed global_position, processes new events through registered projections, and
updates projection_checkpoints. It must be fault-tolerant: if a projection handler fails, the
daemon must log the error, skip the offending event (with configurable retry count), and
continue. A daemon that crashes on a bad event is a production incident.
class ProjectionDaemon:
def __init__(self, store: EventStore, projections: list[Projection]):
self._store = store
self._projections = {p.name: p for p in projections}
self._running = False

async def run_forever(self, poll_interval_ms: int = 100) -> None:
self._running = True
while self._running:
await self._process_batch()
await asyncio.sleep(poll_interval_ms / 1000)

async def _process_batch(self) -> None:
# Load lowest checkpoint across all projections
# Load events from that position in batches
# For each event, route to subscribed projections

TRP1  ·  Arc 5: Integration & Protocol Architecture
# Update checkpoints after each successful batch
# Expose lag metric: global_position - last_processed_position
## ...

## Required Projections
Projection 1: ApplicationSummary
A read-optimised view of every loan application's current state. Stored as a Postgres table
(one row per application). Updated inline by the daemon as new events arrive.
Table schema:
application_id, state, applicant_id,
requested_amount_usd, approved_amount_usd,
risk_tier, fraud_score,
compliance_status, decision,
agent_sessions_completed[],
last_event_type, last_event_at,
human_reviewer_id, final_decision_at

Projection 2: AgentPerformanceLedger
Aggregated performance metrics per AI agent model version. Enables the question: "Has
agent v2.3 been making systematically different decisions than v2.2?"
Table schema:
agent_id, model_version,
analyses_completed, decisions_generated,
avg_confidence_score, avg_duration_ms,
approve_rate, decline_rate, refer_rate,
human_override_rate,
first_seen_at, last_seen_at
Projection 3: ComplianceAuditView (Critical)
This projection is the regulatory read model — the view that a compliance officer or regulator
queries when examining an application. It must be complete (every compliance event),
traceable (every rule references its regulation version), and temporally queryable (state at
any past timestamp).
Unlike the other projections, the ComplianceAuditView must support the temporal query
interface: get_state_at(application_id, timestamp) → ComplianceAuditView. This requires a
snapshot strategy you must implement and justify in DESIGN.md.
● get_current_compliance(application_id) → full compliance record with all checks,
verdicts, and regulation versions
● get_compliance_at(application_id, timestamp) → compliance state as it existed at
a specific moment (regulatory time-travel)
● get_projection_lag() → milliseconds between latest event in store and latest event
this projection has processed — must be exposed as a metric
● rebuild_from_scratch() → truncate projection table and replay all events from
position 0 — must complete without downtime to live reads

Projection Lag — The Non-Negotiable Metric

TRP1  ·  Arc 5: Integration & Protocol Architecture
## The Lag Contract
Your ApplicationSummary projection must maintain a lag of under 500ms in normal operation.
Your ComplianceAuditView projection may lag up to 2 seconds. These are not arbitrary numbers
— they are service-level objectives (SLOs) you define in your DESIGN.md and demonstrate in
testing. A projection system with no lag measurement is not production-ready. Your daemon
must expose get_lag() for every projection it manages, and your test suite must assert that lag
stays within bounds under a simulated load of 50 concurrent command handlers.

PHASE 4  ·  Upcasting, Integrity & The Gas Town Memory Pattern
4A — Upcaster Registry
Implement a centralized UpcasterRegistry that automatically applies version migrations
whenever old events are loaded from the store. The event loading path must call the registry
transparently — callers never manually invoke upcasters.
class UpcasterRegistry:
def __init__(self):
self._upcasters: dict[tuple[str, int], Callable] = {}

def register(self, event_type: str, from_version: int):
"""Decorator. Registers fn as upcaster from event_type@from_version."""
def decorator(fn: Callable[[dict], dict]) -> Callable:
self._upcasters[(event_type, from_version)] = fn
return fn
return decorator

def upcast(self, event: StoredEvent) -> StoredEvent:
"""Apply all registered upcasters for this event type in version order."""
current = event
v = event.event_version
while (event.event_type, v) in self._upcasters:
new_payload = self._upcasters[(event.event_type, v)](current.payload)
current = current.with_payload(new_payload, version=v + 1)
v += 1
return current

## # Usage:
registry = UpcasterRegistry()

@registry.register("CreditAnalysisCompleted", from_version=1)
def upcast_credit_v1_to_v2(payload: dict) -> dict:
return {
## **payload,
"model_version": "legacy-pre-2026",   # inference for historical events
"confidence_score": None,              # genuinely unknown — do not fabricate
## }

Implement upcasters for the following events and justify your inference strategy for missing
historical fields in DESIGN.md:
- CreditAnalysisCompleted v1→v2: Add model_version (inferred from recorded_at
timestamp), confidence_score (null — genuinely unknown; document why fabrication
would be worse than null), regulatory_basis (infer from rule versions active at
recorded_at date).
- DecisionGenerated v1→v2: Add model_versions{} dict (reconstruct from
contributing_agent_sessions by loading each session's AgentContextLoaded event
— this requires a store lookup; document the performance implication).

TRP1  ·  Arc 5: Integration & Protocol Architecture

## The Immutability Test
Your test suite must include a test that: (1) directly queries the events table in Postgres to get the
raw stored payload of a v1 event, (2) loads the same event through your
EventStore.load_stream() and verifies it is upcasted to v2, (3) directly queries the events table
again and verifies the raw stored payload is UNCHANGED. Any system where upcasting
touches the stored events has broken the core guarantee of event sourcing. This test is
mandatory and will be run during assessment.
4B — Cryptographic Audit Chain
Regulatory-grade audit trails require tamper evidence. Implement a hash chain over the
event log for the AuditLedger aggregate. Each AuditIntegrityCheckRun event records a hash
of all preceding events plus the previous integrity hash, forming a blockchain-style chain.
Any post-hoc modification of events breaks the chain.
async def run_integrity_check(
store: EventStore,
entity_type: str,
entity_id: str,
) -> IntegrityCheckResult:
## """
- Load all events for the entity's primary stream
- Load the last AuditIntegrityCheckRun event (if any)
- Hash the payloads of all events since the last check
- Verify hash chain: new_hash = sha256(previous_hash + event_hashes)
- Append new AuditIntegrityCheckRun event to audit-{entity_type}-{entity_id} stream
- Return result with: events_verified, chain_valid (bool), tamper_detected (bool)
## """

4C — The Gas Town Agent Memory Pattern
Implement the pattern that prevents the catastrophic memory loss described in the program
materials. An AI agent that crashes mid-session must be able to restart and reconstruct its
exact context from the event store, then continue where it left off without repeating
completed work.
async def reconstruct_agent_context(
store: EventStore,
agent_id: str,
session_id: str,
token_budget: int = 8000,
) -> AgentContext:
## """
- Load full AgentSession stream for agent_id + session_id
- Identify: last completed action, pending work items, current application state
- Summarise old events into prose (token-efficient)
- Preserve verbatim: last 3 events, any PENDING or ERROR state events
- Return: AgentContext with context_text, last_event_position,
pending_work[], session_health_status

CRITICAL: if the agent's last event was a partial decision (no corresponding
completion event), flag the context as NEEDS_RECONCILIATION — the agent
must resolve the partial state before proceeding.
## """

Test this pattern with a simulated crash: start an agent session, append 5 events, then call
reconstruct_agent_context() without the in-memory agent object. Verify that the
reconstructed context contains enough information for the agent to continue correctly.

TRP1  ·  Arc 5: Integration & Protocol Architecture

PHASE 5  ·  MCP Server — Exposing The Ledger as Enterprise
## Infrastructure
The MCP server is the interface between The Ledger and any AI agent or enterprise system
that needs to interact with it. Tools (Commands) write events; Resources (Queries) read
from projections. This is structural CQRS — the MCP specification naturally implements the
read/write separation.
MCP Tools — The Command Side
## TOOL NAME COMMAND IT
## EXECUTES
## CRITICAL VALIDATION RETURN VALUE
submit_applicati
on
ApplicationSubmitt
ed
Schema validation via
Pydantic; duplicate
application_id check
stream_id, initial_version
record_credit_a
nalysis
CreditAnalysisCo
mpleted
agent_id must have
active AgentSession with
context loaded; optimistic
concurrency on loan
stream
event_id, new_stream_version
record_fraud_sc
reening
FraudScreeningC
ompleted
Same agent session
validation; fraud_score
must be 0.0–1.0
event_id, new_stream_version
record_complia
nce_check
ComplianceRuleP
assed /
ComplianceRuleF
ailed
rule_id must exist in
active
regulation_set_version
check_id, compliance_status
generate_decisi
on
DecisionGenerate
d
All required analyses
must be present;
confidence floor
enforcement
decision_id, recommendation
record_human_
review
HumanReviewCo
mpleted
reviewer_id
authentication; if
override=True,
override_reason required
final_decision, application_state
start_agent_ses
sion
AgentContextLoad
ed
Gas Town: required
before any agent decision
tools; writes context
source and token count
session_id, context_position
run_integrity_ch
eck
AuditIntegrityChec
kRun
Can only be called by
compliance role; rate-
limited to 1/minute per
entity
check_result, chain_valid

MCP Resources — The Query Side
Resources expose projections. They must never load aggregate streams — all reads must
come from projections. A resource that replays events on every query is an anti-pattern that
will not scale.

TRP1  ·  Arc 5: Integration & Protocol Architecture
## RESOURCE URI PROJECTION
## SOURCE
## SUPPORTS
## TEMPORAL
## QUERY?
## SLO
ledger://applications/{id
## }
ApplicationSummar
y
No — current
state only
p99 < 50ms
ledger://applications/{id
## }/compliance
ComplianceAuditVi
ew
## Yes —
## ?as_of=times
tamp
p99 < 200ms
ledger://applications/{id
## }/audit-trail
AuditLedger stream
(direct load —
justified exception)
## Yes —
## ?from=&to=
range
p99 < 500ms
ledger://agents/{id}/perf
ormance
AgentPerformance
## Ledger
No — current
metrics only
p99 < 50ms
ledger://agents/{id}/ses
sions/{session_id}
AgentSession
stream (direct load)
Yes — full
replay
capability
p99 < 300ms
ledger://ledger/health ProjectionDaemon.
get_all_lags()
No p99 < 10ms — this is the watchdog
endpoint

Tool Interface Design for LLM Consumption
Tools and resources are consumed by AI agents, not humans. The description and
parameter schema of each tool determines whether the consuming LLM uses it correctly —
this is API design for a non-human consumer. Two requirements that most engineers miss:
‣ Precondition documentation in the tool description: "This tool requires an active
agent session created by start_agent_session. Calling without an active session will
return a PreconditionFailed error." An LLM that does not know this precondition will
repeatedly fail and retry. The description is the only contract the LLM has.
‣ Structured error types, not messages: Errors returned by tools must be typed
objects: {error_type: "OptimisticConcurrencyError", message: "...", stream_id: "...",
expected_version: 3, actual_version: 5, suggested_action:
"reload_stream_and_retry"}. An LLM that receives an unstructured error message
cannot reason about what to do. A typed error with suggested_action enables
autonomous recovery.

The MCP Integration Test
Your MCP server must pass this integration test: start a fresh Ledger instance, then drive a
complete loan application lifecycle — from ApplicationSubmitted through FinalApproved — using
only MCP tool calls. No direct Python function calls. The test simulates what a real AI agent
would do: it calls start_agent_session, then record_credit_analysis, then generate_decision, then
record_human_review, then queries the compliance audit view to verify the complete trace is
present. If any step requires a workaround outside the MCP interface, the interface has a design
flaw.

PHASE 6 (BONUS)  ·  What-If Projections & Regulatory Time Travel
This phase is required for Score 5 and is the discriminator for trainees with genuine event
sourcing experience. It is challenging and takes a full day. Attempt it only after Phases 1–5
are solid.

TRP1  ·  Arc 5: Integration & Protocol Architecture
The What-If Projector
The Apex compliance team needs to run counterfactual scenarios: "What would the decision
have been if we had used the March risk model instead of the February risk model?" This
requires replaying application history with a substituted event — a counterfactual — injected
at the point of the original credit analysis.
async def run_what_if(
store: EventStore,
application_id: str,
branch_at_event_type: str,            # e.g. "CreditAnalysisCompleted"
counterfactual_events: list[BaseEvent],  # events to inject instead of real ones
projections: list[Projection],        # projections to evaluate under the scenario
) -> WhatIfResult:
## """
- Load all events for the application stream up to the branch point
- At the branch point, inject counterfactual_events instead of real events
- Continue replaying real events that are causally INDEPENDENT of the branch
- Skip real events that are causally DEPENDENT on the branched events
- Apply all events (pre-branch real + counterfactual + post-branch independent)
to each projection
- Return: {real_outcome, counterfactual_outcome, divergence_events[]}

NEVER writes counterfactual events to the real store.
Causal dependency: an event is dependent if its causation_id traces
back to an event at or after the branch point.
## """

Demonstrate with the specific scenario: "What would the final decision have been if the
credit analysis had returned risk_tier='HIGH' instead of 'MEDIUM'?" Your what-if projector
must produce a materially different ApplicationSummary outcome — demonstrating that
business rule enforcement cascades correctly through the counterfactual.

## Regulatory Examination Package
Implement a generate_regulatory_package(application_id, examination_date) function that
produces a complete, self-contained examination package containing:
- The complete event stream for the application, in order, with full payloads.
- The state of every projection as it existed at examination_date.
- The audit chain integrity verification result.
- A human-readable narrative of the application lifecycle, generated by replaying
events and constructing a plain-English summary (one sentence per significant
event).
- The model versions, confidence scores, and input data hashes for every AI agent
that participated in the decision.
The package must be a self-contained JSON file that a regulator can verify against the
database independently — they should not need to trust your system to validate that the
package is accurate.

DESIGN.md — Required Sections
This document is assessed with equal weight to the code. The principle: architecture is
about tradeoffs. A decision without a tradeoff analysis is not an architectural decision — it is
a default. Six required sections:


TRP1  ·  Arc 5: Integration & Protocol Architecture
- Aggregate boundary justification: Why is ComplianceRecord a separate
aggregate from LoanApplication? What would couple if you merged them? Trace the
coupling to a specific failure mode under concurrent write scenarios.
- Projection strategy: For each projection, justify: Inline vs. Async, and the SLO
commitment. For the ComplianceAuditView temporal query, justify your snapshot
strategy (event-count trigger, time trigger, or manual) and describe snapshot
invalidation logic.
- Concurrency analysis: Under peak load (100 concurrent applications, 4 agents
each), how many OptimisticConcurrencyErrors do you expect per minute on the loan-
{id} streams? What is the retry strategy and what is the maximum retry budget before
you return a failure to the caller?
- Upcasting inference decisions: For every inferred field in your upcasters, quantify
the likely error rate and the downstream consequence of an incorrect inference.
When would you choose null over an inference?
- EventStoreDB comparison: Map your PostgreSQL schema to EventStoreDB
concepts: streams → stream IDs, your load_all() → EventStoreDB $all stream
subscription, your ProjectionDaemon → EventStoreDB persistent subscriptions. What
does EventStoreDB give you that your implementation must work harder to achieve?
- What you would do differently: Name the single most significant architectural
decision you would reconsider with another full day. This section is the most
important — it shows whether you can distinguish between "what I built" and "what
the best version of this would be."

## Deliverables
Interim — Sunday March 22, 03:00 UTC
GitHub Code:
● src/schema.sql — PostgreSQL schema: events, event_streams,
projection_checkpoints, outbox tables with all indexes and constraints
● src/event_store.py — EventStore async class with append, load_stream,
load_all, stream_version, archive_stream, get_stream_metadata;
optimistic concurrency enforced via expected_version
● src/models/events.py — Pydantic models for all event types in the Event
Catalogue (BaseEvent, StoredEvent, StreamMetadata) plus custom exceptions
(OptimisticConcurrencyError, DomainError)
● src/aggregates/loan_application.py — LoanApplicationAggregate
with state machine, event replay via load(), and _apply handlers for all loan
lifecycle events
● src/aggregates/agent_session.py — AgentSessionAggregate with Gas
Town context enforcement and model version tracking

TRP1  ·  Arc 5: Integration & Protocol Architecture
● src/commands/handlers.py — Command handlers following the load → validate
→ determine → append pattern (at minimum:
handle_credit_analysis_completed, handle_submit_application)
● tests/test_concurrency.py — Double-decision concurrency test: two
concurrent asyncio tasks appending to the same stream at
expected_version=3; asserts exactly one succeeds, one raises
OptimisticConcurrencyError, and total stream length = 4
● pyproject.toml with locked deps (uv)
● README.md — how to install, run migrations, and execute the test suite
Single PDF Report containing:
- DOMAIN_NOTES.md content (complete, as graded deliverable)
- Architecture diagram showing event store schema, aggregate boundaries, and
command flow
- Progress summary: what is working (Phase 1 + Phase 2), what is in progress
- Concurrency test results: screenshot or log output of the double-decision test passing
- Known gaps and plan for final submission
Final — Thursday March 26, 03:00 UTC
GitHub Code (full system):
## Phase 1 — Event Store Core:
● src/schema.sql — Full PostgreSQL schema with all tables, indexes, and
constraints
● src/event_store.py — Complete EventStore async class with all interface
methods, outbox writes in same transaction, stream archival support
● src/models/events.py — All Pydantic models: event types, stored event
wrapper, stream metadata, error types
## Phase 2 — Domain Logic:
● src/aggregates/loan_application.py — LoanApplicationAggregate
with full state machine (Submitted → AwaitingAnalysis → AnalysisComplete →
ComplianceReview → PendingDecision → ApprovedPendingHuman /
DeclinedPendingHuman → FinalApproved / FinalDeclined), all 6 business rules
enforced
● src/aggregates/agent_session.py — AgentSessionAggregate with Gas
Town context enforcement, model version locking

TRP1  ·  Arc 5: Integration & Protocol Architecture
● src/aggregates/compliance_record.py — ComplianceRecordAggregate
with mandatory check tracking and regulation version references
● src/aggregates/audit_ledger.py — AuditLedgerAggregate with append-
only enforcement and cross-stream causal ordering
● src/commands/handlers.py — All command handlers: submit_application,
credit_analysis_completed, fraud_screening_completed, compliance_check,
generate_decision, human_review_completed, start_agent_session
## Phase 3 — Projections & Async Daemon:
● src/projections/daemon.py — ProjectionDaemon with fault-tolerant batch
processing, per-projection checkpoint management, configurable retry, and
get_lag() per projection
● src/projections/application_summary.py — ApplicationSummary
projection (one row per application, current state)
● src/projections/agent_performance.py — AgentPerformanceLedger
projection (metrics per agent model version)
● src/projections/compliance_audit.py — ComplianceAuditView
projection with temporal query support (get_compliance_at(application_id,
timestamp)), snapshot strategy, and rebuild_from_scratch()
## Phase 4 — Upcasting, Integrity & Gas Town:
● src/upcasting/registry.py — UpcasterRegistry with automatic version
chain application on event load
● src/upcasting/upcasters.py — Registered upcasters:
CreditAnalysisCompleted v1→v2, DecisionGenerated v1→v2, with inference
strategies documented
● src/integrity/audit_chain.py — run_integrity_check(): SHA-256
hash chain construction, tamper detection, chain verification
● src/integrity/gas_town.py — reconstruct_agent_context(): agent
memory reconstruction from event stream with token budget,
NEEDS_RECONCILIATION detection
Phase 5 — MCP Server:
● src/mcp/server.py — MCP server entry point
● src/mcp/tools.py — 8 MCP tools (command side): submit_application,
record_credit_analysis, record_fraud_screening,
record_compliance_check, generate_decision, record_human_review,
start_agent_session, run_integrity_check; all with structured error types
and precondition documentation in tool descriptions
● src/mcp/resources.py — 6 MCP resources (query side):
ledger://applications/{id},

TRP1  ·  Arc 5: Integration & Protocol Architecture
ledger://applications/{id}/compliance,
ledger://applications/{id}/audit-trail,
ledger://agents/{id}/performance,
ledger://agents/{id}/sessions/{session_id},
ledger://ledger/health; all reading from projections (no stream replays except
justified exceptions)
Phase 6 (Bonus):
● src/what_if/projector.py — run_what_if(): counterfactual event injection
with causal dependency filtering, never writes to real store
● src/regulatory/package.py — generate_regulatory_package(): self-
contained JSON examination package with event stream, projection states at
examination date, integrity verification, human-readable narrative, and agent model
metadata
## Tests:
● tests/test_concurrency.py — Double-decision test (two concurrent appends,
exactly one succeeds)
● tests/test_upcasting.py — Immutability test: v1 event stored, loaded as v2
via upcaster, raw DB payload confirmed unchanged
● tests/test_projections.py — Projection lag SLO tests under simulated load
of 50 concurrent command handlers; rebuild_from_scratch test
● tests/test_gas_town.py — Simulated crash recovery: 5 events appended,
reconstruct_agent_context() called without in-memory agent, verify
reconstructed context is sufficient to continue
● tests/test_mcp_lifecycle.py — Full loan application lifecycle driven entirely
through MCP tool calls: start_agent_session → record_credit_analysis
→ record_fraud_screening → record_compliance_check →
generate_decision → record_human_review → query
ledger://applications/{id}/compliance to verify complete trace
● pyproject.toml with locked deps (uv)
● README.md — Full setup instructions: database provisioning, migration, running all
phases, MCP server startup, and query examples

Single PDF Report containing:
- DOMAIN_NOTES.md content (complete, finalized)
- DESIGN.md content (complete, finalized)
- Architecture diagram: event store schema, aggregate boundaries, projection data
flow, MCP tool/resource mapping

TRP1  ·  Arc 5: Integration & Protocol Architecture
- Concurrency & SLO analysis: double-decision test results, projection lag
measurements under load, retry budget analysis
- Upcasting & integrity results: immutability test output, hash chain verification output,
tamper detection demonstration
- MCP lifecycle test results: full loan application trace from ApplicationSubmitted
through FinalApproved via MCP tools only
- Bonus results (if attempted): what-if counterfactual outcome comparison, regulatory
package sample output
- Limitations & reflection: what the implementation does not handle, what you would
change with more time
Video Demo (max 6 min):
Minutes 1–3 (Required):
● Step 1 — The Week Standard: Run "Show me the complete decision history of
application ID X" end-to-end. Show full event stream, all agent actions, compliance
checks, human review, causal links, and cryptographic integrity verification. Time it
— must complete in under 60 seconds.
● Step 2 — Concurrency Under Pressure: Run the double-decision test live. Show
two agents colliding on the same stream, one succeeding, one receiving
OptimisticConcurrencyError and retrying.
## ● Step 3 — Temporal Compliance Query: Query
ledger://applications/{id}/compliance?as_of={timestamp} for a past
point in time. Show the compliance state as it existed at that moment, distinct from
the current state.
Minutes 4–6 (Mastery):
● Step 4 — Upcasting & Immutability: Load a v1 event through the store, show it
arrives as v2. Query the raw database row and show the stored payload is
unchanged.
● Step 5 — Gas Town Recovery: Start an agent session, append several events,
simulate a crash (kill the process). Call reconstruct_agent_context() and
show the agent can resume with correct state.
● Step 6 — What-If Counterfactual (Bonus): Run a what-if scenario substituting a
HIGH risk tier for MEDIUM. Show the cascading effect on the final decision through
business rule enforcement.

## Assessment Rubric
Score 3 = functional and demonstrates understanding. Score 5 = production-ready, would deploy
to a real enterprise client. Scores 4 and 5 require demonstrated understanding in DESIGN.md,
not just working code.


TRP1  ·  Arc 5: Integration & Protocol Architecture
## CRITERION 1 2 3 4 5
## Event Store Core & Concurrency Schema
present; no
concurrency
control
## Append
works;
expected_ve
rsion not
enforced
All interface
methods;
concurrency
enforced;
double-
decision test
passes
## Outbox
pattern;
archive
support; all
edge cases;
concurrent
load test
passes
## Above +
DESIGN.md
justifies every
schema
column; retry
strategy
documented
with error rate
estimate
## Domain Logic & Business Rules One
aggregate;
no state
machine
## State
machine
present;
some rules
missing
## Both
aggregates;
all 6
business
rules
enforced
Causal chain
enforcement
## ; Gas Town
pattern;
model
version
locking
## Above +
counterfactual
command
testing; all
invariants
tested under
concurrent
scenarios
Projection Daemon & CQRS No
projections
or direct
stream
reads only
## One
projection;
no lag metric
## All 3
projections;
lag metric
exposed;
daemon
fault-tolerant
SLO tests
passing;
rebuild-from-
scratch
without
downtime;
temporal
query on
## Compliance
AuditView
## Above +
snapshot
invalidation;
distributed
daemon
analysis in
DESIGN.md
## Upcasting & Integrity No
upcasting;
store
mutated on
upcast
## Upcaster
exists; chain
not
automatic
## Auto-
upcasting
via registry;
immutability
test passes
## Both
upcasters;
inference
justified; null
vs.
fabrication
reasoning
present
Above + hash
chain integrity;
generate_regul
atory_package
working; chain
break detection
MCP Server — Tool Design No MCP
server
## Tools
present;
error types
unstructured
All 8 tools;
structured
errors;
precondition
s
documented
## Resources
from
projections
(no stream
reads in
resources);
SLOs met
Above + full
lifecycle
integration test
via MCP only;
## LLM-
consumption
preconditions
in all tool
descriptions
DESIGN.md — Architectural
## Reasoning
Not present Describes
what was
built; no
tradeoff
analysis
## All 6
sections;
tradeoffs
identified
## Quantitative
analysis
(error rates,
lag SLOs,
retry
budgets)
"What I would
do differently"
shows genuine
reflection;
identifies the
thing the
implementation
got wrong
BONUS — What-If & Regulatory
## Package
(Not
attempted)
(Not
attempted)
## What-if
projection
working on
test scenario
## Counterfactu
al produces
materially
different
outcome;
causal
dependency
## Above +
regulatory
package is
independently
verifiable;
narrative

TRP1  ·  Arc 5: Integration & Protocol Architecture
## CRITERION 1 2 3 4 5
filtering
correct
generation
coherent




