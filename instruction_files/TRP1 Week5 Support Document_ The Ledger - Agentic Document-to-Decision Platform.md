

TRP1  · The Ledger  ·  Challenge Document March 2026
Page 1  ·  Confidential  ·  TRP1 FDE Program

## TRP1 FDE PROGRAM  ·  ARC 5–6  ·  WEEK 5
## The Ledger
Agentic Document-to-Decision Platform
Two weeks. Five real LangGraph agents. GAAP financial documents. A full event-sourced loan decisioning
pipeline — from uploaded PDF to auditable approval or decline.

## SECTION 1  ·  WHAT YOU ARE BUILDING AND WHY

"Apex Financial Services processes 40–80 commercial loan applications per
week. Applicants upload financial statements. AI agents read them, reason about
them, and record every decision as an immutable event. Nothing is lost.
Everything is auditable. You are building the infrastructure that makes this
possible."

This is not a textbook exercise. The system you build has five components that must interlock correctly — and you
do not control the order in which they break.

## COMPONENT WHAT IT IS WHERE ITS DATA
## LIVES
## YOUR JOB
## Applicant
## Registry
A   read-only   PostgreSQL
CRM.   80   companies,   3
years  of  GAAP  financials,
compliance flags, loan
history.
External schema:
applicant_registry.*Ne
ver written by the event
store system.
Query it (read-only) from agents. Never append
to it.
## Document
## Corpus
160+ files per 80
applicants: income
statement    PDF,    balance
sheet PDF, multi-year
Excel  workbook,  flat  CSV.
All GAAP-formatted.
Generated    by    the    data
generator.
## Filesystem:
documents/{company_
id}/File  paths  recorded
in   DocumentUploaded
events.
Plug   your   Week   3   extraction   pipeline   into
DocumentProcessingAgent.
Event Store Append-only   PostgreSQL
table. Seven aggregate
stream     types.     ~3,500+
events    by    end    of the
project.
Event     store     schema:
events,   event_streams,
outbox, snapshots,
projection_checkpoints
.The   single   source   of
truth  for  all  application
lifecycle decisions.
Implement EventStore.append(),
load_stream(),   load_all().   Every   agent   and
projection depends on this.
LangGraph
## Agents
Five  compiled  StateGraph
agents. Every node
execution, every LLM call,
every tool call recorded as
an   event   in   the   agent's
session stream.
Agent  session  streams:
agent-{type}-
{session_id}Output
streams: loan-*,
docpkg-*, credit-*,
fraud-*, compliance-*
Implement  the  four  stub  agents  following  the
CreditAnalysisAgent reference pattern.
## Projections +
## MCP
Three read-model
projections    rebuilt    from
the   event   stream.   MCP
server    exposes    8    tools
(commands) and 6
Projection  tables  in  the
same PostgreSQL
database.MCP served
locally on port 8765.
Build ProjectionDaemon and three projections.
Expose via FastMCP.

TRP1  · The Ledger  ·  Challenge Document March 2026
Page 2  ·  Confidential  ·  TRP1 FDE Program
## COMPONENT WHAT IT IS WHERE ITS DATA
## LIVES
## YOUR JOB
resources (projection
queries).

The Two Non-Negotiable Design Rules
Before writing a single line of agent code, internalise these two rules. Every architectural decision in this system
flows from them.

## Rule 1:
## The Data
## Boundary
The Applicant Registry is a read-only external system. Agents query it; they never write to
it. Historical financial data, compliance flags, and company profiles live there because they
existed  before  any  loan  application  was  submitted.  The  event  store  captures  only  what
happens during the application lifecycle — everything from submission to decision. If you
find  yourself  wanting  to  write  to  the  Applicant  Registry  from  an  agent,  you  have
misunderstood the boundary.

## Rule 2:
## Gas Town
## — Session
## Start
## Before
## Any Work
Every agent appends AgentSessionStarted as the very first event, before any data is loaded
or  any  decision  is made.  This  is the  Gas  Town  pattern:  the  session  stream  is the  agent's
memory. On crash recovery, a new agent instance replays its session stream to reconstruct
context and resume from the last successful node — without redoing completed work. An
agent that starts work before appending AgentSessionStarted cannot recover from a crash.



TRP1  · The Ledger  ·  Challenge Document March 2026
Page 3  ·  Confidential  ·  TRP1 FDE Program
## SECTION 2  ·  THE CANONICAL EVENT SCHEMA — ALL 7 AGGREGATES

All 45 event types are defined in ledger/schema/events.py. That file is the single source of truth. Every agent, every
test, every projection imports from there. Never redefine event classes elsewhere.
The data generator (datagen/event_simulator.py) simulates all 45 event types for seed applications and validates
every  generated  event  against  EVENT_REGISTRY  before  writing  to  the  database.  If  your  schema  changes,  the
generator catches it immediately.

## STREAM
## PREFIX
AGGREGATE KEY EVENTS (in lifecycle order) WHO WRITES
loan-{id} LoanApplication ApplicationSubmitted ·
DocumentUploadRequested ·
DocumentUploaded ·
CreditAnalysisRequested ·
FraudScreeningRequested ·
ComplianceCheckRequested ·
DecisionRequested   ·   DecisionGenerated   ·
HumanReviewRequested ·
HumanReviewCompleted ·
ApplicationApproved · ApplicationDeclined
Command  handlers  +  agents  (as
side-effects of completing work)
docpkg-{id} DocumentPackage PackageCreated · DocumentAdded ·
DocumentFormatValidated ·
ExtractionStarted   ·   ExtractionCompleted   ·
QualityAssessmentCompleted ·
PackageReadyForAnalysis
DocumentProcessingAgent
exclusively
agent-{type}-
## {session_id}
AgentSession AgentSessionStarted ·  AgentInputValidated ·
AgentInputValidationFailed ·
AgentNodeExecuted   (one   per    LangGraph
node) · AgentToolCalled (one per
registry/store query) ·  AgentOutputWritten ·
AgentSessionCompleted ·
AgentSessionFailed · AgentSessionRecovered
Each   agent   writes   to   its   own
session stream
credit-{id} CreditRecord CreditRecordOpened ·
HistoricalProfileConsumed ·
ExtractedFactsConsumed ·
CreditAnalysisCompleted ·
CreditAnalysisDeferred
CreditAnalysisAgent exclusively
fraud-{id} FraudScreening FraudScreeningInitiated ·
FraudAnomalyDetected (0–N) ·
FraudScreeningCompleted
FraudDetectionAgent exclusively
compliance-
## {id}
ComplianceRecor
d
ComplianceCheckInitiated ·
ComplianceRulePassed/Failed/Noted (one
per rule) · ComplianceCheckCompleted
ComplianceAgent exclusively
audit-
## {entity_type}-
## {id}
AuditLedger AuditIntegrityCheckRun (with SHA-256 hash
chain)
Audit chain builder (Phase 4)

The AgentSession Stream — One Event Per LangGraph Node
This is the most important structural requirement of the system. Every time a LangGraph node executes, your agent
must append an AgentNodeExecuted event to its session stream. This means the session stream is a complete, node-
by-node record of the agent's execution — suitable for regulatory examination, crash recovery, and cost attribution.


TRP1  · The Ledger  ·  Challenge Document March 2026
Page 4  ·  Confidential  ·  TRP1 FDE Program
## EVENT TYPE WHEN APPENDED KEY FIELDS REQUIRED?
AgentSessionStarte
d
First — before   any   data
loaded  or  decision  made.
Gas Town anchor.
session_id, agent_type,
model_version, context_source
("fresh" or
## "prior_session_replay:{id}"),
context_token_count
REQUIRED — every session
AgentInputValidate
d
After validate_inputs node
succeeds.
inputs_validated   (list   of   what   was
checked), validation_duration_ms
REQUIRED — every session
AgentInputValidati
onFailed
If    validate_inputs    finds
missing or invalid inputs.
missing_inputs (list),
validation_errors (list)
Required    when    inputs    are
invalid
AgentNodeExecute
d
At the END of every node.
One per node per session.
node_name, node_sequence,
input_keys, output_keys, llm_called,
llm_tokens_input,
llm_tokens_output, llm_cost_usd,
duration_ms
REQUIRED — every node
AgentToolCalled After   every   call   to   the
Applicant Registry or
event   store   (as   a   query
tool).
tool_name, tool_input_summary
(condensed),  tool_output_summary
(condensed), tool_duration_ms
Required per tool call
AgentOutputWritte
n
After   write_output   node
appends all domain
events.
events_written   (list   of   {stream_id,
event_type, stream_position}),
output_summary
REQUIRED — every session
AgentSessionComp
leted
Last event — after all work
is done.
total_nodes_executed,
total_llm_calls,    total_tokens_used,
total_cost_usd,
next_agent_triggered
REQUIRED — every session
AgentSessionFailed On unrecoverable error. error_type, error_message,
last_successful_node, recoverable
## (bool)
Required on failure
AgentSessionRecov
ered
First  event  of  a  recovery
session.
recovered_from_session_id,
recovery_point   (node   name   where
resumed)
Required on recovery



TRP1  · The Ledger  ·  Challenge Document March 2026
Page 5  ·  Confidential  ·  TRP1 FDE Program
## SECTION 3  ·  THE DATA GENERATOR — RUN FIRST, EVERYTHING DEPENDS ON IT

The  data  generator  is  a  full  deliverable.  It  creates  three  distinct  datasets  before  any  agent  runs:  the  Applicant
Registry database, the Document Corpus, and the seed event history. Run it once on Day 1. If it exits with code 0,
your environment is ready.

# Run once. Idempotent — safe to re-run (ON CONFLICT DO NOTHING everywhere).
python datagen/generate_all.py \
## --applicants 80 \
--db-url postgresql://localhost/apex_ledger \
## --docs-dir ./documents \
## --output-dir ./data \
## --random-seed 42

# Expected output (abridged):
# [1/5] Generating 80 company profiles...
## #   [OK] GROWTH:20, STABLE:25, DECLINING:12, RECOVERING:13, VOLATILE:10
#   [OK] LOW:24, MEDIUM:33, HIGH:23  |  With compliance flags: 8
# [2/5] Generating financial documents...
#   [OK] 320 files in ./documents/  (80 income PDFs + 80 balance PDFs + 80 Excel + 80
## CSV)
# [3/5] Simulating seed event history (29 applications)...
#   [OK] 1,847 events across 29 applications validated
# [4/5] Schema validation: 1847 validated, 0 errors
# [5/5] Writing to database...
#   [OK] Database write complete
# GENERATION COMPLETE in 4m 18s

What the Generator Creates
## DATASET FILES / TABLES COUNT PURPOSE
## Applicant
## Registry
applicant_registry.compa
niesapplicant_registry.fin
ancial_historyapplicant_r
egistry.compliance_flagsa
pplicant_registry.loan_rel
ationships
## 80
companies
## 240
financial
rows (3yr
each)~8
flag
rows~48
loan rows
Read-only   source   of   company   profiles   and   historical
financials. Agents query this; never write to it.
## GAAP     PDF —
## Income
## Statement
documents/{company_id}
## /income_statement_2024
## .pdf
80  PDFs  in
4   variants:
clean   (40),
dense/mult
i-subtotal
## (20),
missing-
## EBITDA
## (8),
scanned-
quality (12)
Primary input to Week 3 extraction pipeline. Each variant
exercises a different extraction challenge.
## GAAP     PDF —
## Balance Sheet
documents/{company_id}
## /balance_sheet_2024.pdf
80  PDFs.  6
intentionall
y contain
minor
equity
rounding
discrepancy
## ($500–
## $4,500).
Tests balance_sheet_balances validation.
DocumentProcessingAgent must flag discrepancies.

TRP1  · The Ledger  ·  Challenge Document March 2026
Page 6  ·  Confidential  ·  TRP1 FDE Program
## DATASET FILES / TABLES COUNT PURPOSE
Excel Workbook documents/{company_id}
## /financial_statements.xls
x
## 80
workbooks.
4 sheets:
## Income
## Statement,
## Balance
## Sheet,    Key
Ratios,  and
a 3-year
comparison
## .
Alternative  format  input.  DocumentProcessingAgent  can
process .xlsx as well as PDF.
## Application
Proposal PDF
Generated  per  application
(not per company)
One per
seeded
application
Contains    company    narrative,    loan    purpose,    use    of
proceeds, GAAP financial highlights. Tests the
APPLICATION_PROPOSAL document type.
Flat CSV documents/{company_id}
## /financial_summary.csv
80 CSVs
## (most
recent fiscal
year only)
Quick programmatic consumption of financials. Useful for
registry cross-reference checks in FraudDetectionAgent.
## Seed Events
## (JSONL)
data/seed_events.jsonl    +
database
## 1,847
events
across 29
application
s
Full realistic event history for 29 applications in 9 lifecycle
states.  Simulates  all  5  LangGraph  agents  including  per-
node  AgentNodeExecuted  events.  Validates  the  complete
schema before you write any real agent code.

Seed Application Distribution (29 applications)
APPLICATION IDs STATE COU
## NT
## WHAT AGENTS CAN DO WITH THEM
## APEX-0001 – APEX-
## 0006
SUBMITTEDDocu
ments requested,
nothing uploaded
## 6
Upload documents via MCP tool. Tests DocumentUploadRequested
→ DocumentUploaded flow.
## APEX-0007 – APEX-
## 0011
## DOCUMENTS_UP
LOADEDDocs on
disk,   Week   3   not
run
5 Run  DocumentProcessingAgent  immediately.  Tests  the  standard
entry path.
## APEX-0012 – APEX-
## 0015
## DOCUMENTS_PR
OCESSEDFacts
extracted, credit not
started
4 Run  CreditAnalysisAgent.  FinancialFacts  already  in  events — no
extraction needed.
## APEX-0016 – APEX-
## 0018
## CREDIT_COMPLE
TECredit done,
fraud pending
3 Run FraudDetectionAgent. Tests mid-lifecycle agent pickup.
## APEX-0019 – APEX-
## 0020
## FRAUD_COMPLE
TEFraud done,
compliance not
started
2 Run  ComplianceAgent.  Tests  compliance  evaluation  on  a  live
stream.
## APEX-0021 – APEX-
## 0025
## APPROVED   (4)   +
## DECLINED (1)
5 Projections must reflect terminal states correctly from Day 1.
## APEX-0026 – APEX-
## 0027
DECLINEDAgent-
driven credit
decline
2 Tests   decline   path.   adverse_action_notice_required=True   on
ApplicationDeclined.
## APEX-0028 DECLINED_COMP
LIANCEMontana
company — REG-
003 hard block
1 Tests  compliance  hard  block.  No  DecisionGenerated  should  ever
appear.

TRP1  · The Ledger  ·  Challenge Document March 2026
Page 7  ·  Confidential  ·  TRP1 FDE Program
APPLICATION IDs STATE COU
## NT
## WHAT AGENTS CAN DO WITH THEM
APEX-0029 REFERREDLow-
confidence
orchestrator
decision
1 Tests  human  review  path.  HumanReviewCompleted  command
handler must work on this.

## The
## Simulator
## Validates
## Your
## Schema
The event simulator (datagen/event_simulator.py) mirrors exactly what real LangGraph
agents produce: the same event types, the same field names, the same causal chains, the
same  AgentNodeExecuted  sequence  per  agent.  If  the  simulator  runs  clean  (0  schema
errors), your schema is correctly implemented and your real agents will be able to write to
it. Run: python datagen/generate_all.py --validate-only to check schema without writing
to the database.



TRP1  · The Ledger  ·  Challenge Document March 2026
Page 8  ·  Confidential  ·  TRP1 FDE Program
## SECTION 4  ·  THE FIVE LANGGRAPH AGENTS

Every     agent     is     a     compiled     LangGraph     StateGraph.     Every     agent     inherits     from     BaseApexAgent
(ledger/agents/base_agent.py), which provides Gas Town session management, per-node event recording, tool call
recording,  OCC  retry  scaffolding,  and  LLM  cost  tracking. You  implement  the  nodes;  the  base  class  handles
everything else.

## The
## Reference
## Implement
ation
CreditAnalysisAgent  (ledger/agents/credit_analysis_agent.py)  is  the  complete  reference
implementation.  It  demonstrates:  build_graph()  with  a  6-node  StateGraph,  each  node
calling self._record_node_execution() at its end, the LLM call pattern via self._call_llm(),
the OCC retry pattern in write_output, and how to trigger the next agent. Implement the
remaining 4 agents by following this pattern exactly.

## Node Sequence — All Agents Follow This Pattern
# Every agent has this node sequence (domain nodes vary):

validate_inputs → open_aggregate_record → load_external_data → [domain nodes] →
write_output

# validate_inputs:      Check application state, verify prerequisites exist
# open_aggregate_record: Create the agent's output aggregate stream (e.g.
CreditRecordOpened)
# load_external_data:   Query Applicant Registry AND load from event store
# [domain nodes]:       The reasoning work — usually one LLM call node + one policy node
# write_output:         Append output events with OCC retry; trigger next agent

# At the END of EVERY node, call:
await self._record_node_execution(
node_name="my_node",
input_keys=["key1", "key2"],       # state keys consumed
output_keys=["result_key"],        # state keys produced
duration_ms=int((time.time()-t0)*1000),
llm_tokens_input=tok_in,           # None if no LLM call
llm_tokens_output=tok_out,
llm_cost_usd=cost,
## )

# For every registry or event store query, call:
await self._record_tool_call(
tool="query_applicant_registry",
inp=f"company_id={applicant_id}",
out=f"Loaded 3yr financials, {n} flags",
ms=duration_ms,
## )


Agent 1 — DocumentProcessingAgent (stub_agents.py)
Wraps the Week 3 Document Intelligence pipeline. Takes uploaded PDFs, runs extraction, assesses quality with the
LLM, and appends extraction events to the docpkg stream. The LLM's role is quality assessment only — it checks
coherence, not creditworthiness.
## NODE READS FROM WRITES TO STORE LLM?
validate_inputs LoanApplication
stream  (state  must  be
AgentInputValidated No

TRP1  · The Ledger  ·  Challenge Document March 2026
Page 9  ·  Confidential  ·  TRP1 FDE Program
## NODE READS FROM WRITES TO STORE LLM?
## DOCUMENTS_UPLOA
## DED)
validate_document_
formats
Filesystem  (check  files
exist, check format)
DocumentFormatValidated
per document
## No
extract_income_stat
ement
PDF   file   via   Week   3
pipeline
ExtractionStarted,
ExtractionCompleted (with
FinancialFacts)
No (pipeline handles)
extract_balance_she
et
PDF   file   via   Week   3
pipeline
ExtractionStarted,
ExtractionCompleted (with
FinancialFacts)
## No
assess_quality Extracted
FinancialFacts from
both documents
QualityAssessmentCompleted,
PackageReadyForAnalysis
YES — checks     coherence,     flags
anomalies
write_output State     dict     (decisions
complete)
CreditAnalysisRequested on
loan    stream    (triggers    next
agent)
## No

Week   3   integration: In   _node_extract_income_statement(),   call   your   Week   3   pipeline   directly:   from
document_refinery.pipeline import extract_financial_facts. The extracted FinancialFacts struct goes directly into
the    ExtractionCompleted    event    payload.    If    extraction    produces    None    for    any    critical    field,    set
field_confidence[field] = 0.0 and add to extraction_notes — do not default to zero.

# Quality assessment LLM prompt (implement in _node_assess_quality):
## QUALITY_SYSTEM_PROMPT = """
You are a financial document quality analyst. You receive structured data
extracted from a company's financial statements.

Check ONLY:
- Internal consistency (Gross Profit = Revenue - COGS, Assets = Liabilities + Equity)
- Implausible values (margins > 80%, negative equity without note)
- Critical missing fields (total_revenue, net_income, total_assets, total_liabilities)

Return JSON: {"overall_confidence": float, "is_coherent": bool,
## "anomalies": [str], "critical_missing_fields": [str],
"reextraction_recommended": bool, "auditor_notes": str}

DO NOT make credit or lending decisions. DO NOT suggest loan outcomes.
## """


Agent 2 — CreditAnalysisAgent (credit_analysis_agent.py — REFERENCE)
The  complete  reference  implementation.  Read  it  before  implementing  any other  agent. Six  nodes.  One  LLM call
(analyze_credit_risk node). Two data sources: Applicant Registry (historical financials via registry client) and event
store (extracted facts from docpkg stream). Hard policy constraints enforced in Python after the LLM call — the
LLM cannot override them.
## POLICY RULE ENFORCED IN WHAT IT DOES
Max   loan-to-revenue   ratio:
## 35%
apply_policy_constraints
node (Python)
Reduces recommended_limit_usd if it exceeds
annual_revenue × 0.35. LLM recommendation is overridden.
Prior  default  →  risk_tier  =
## HIGH
apply_policy_constraints
node (Python)
Forces  risk_tier  to  HIGH  regardless  of  LLM  output  if  any
loan_relationship has default_occurred=True.

TRP1  · The Ledger  ·  Challenge Document March 2026
Page 10  ·  Confidential  ·  TRP1 FDE Program
## POLICY RULE ENFORCED IN WHAT IT DOES
Active HIGH compliance flag
→ confidence ≤ 0.50
apply_policy_constraints
node (Python)
Caps  confidence  at  0.50.  Combined  with  the  orchestrator's
confidence < 0.60 → REFER rule, this guarantees human review.
confidence < 0.60 → REFER
LoanApplicationAggregat
e.assert_valid_orchestrat
or_decision()     (aggregate
enforces)
The aggregate rejects DecisionGenerated with
recommendation=APPROVE   if   confidence   <   0.60.   The
orchestrator cannot override this.

Agent 3 — FraudDetectionAgent (stub_agents.py)
Detects  inconsistencies  between  submitted  documents  and  what  the  bank  already  knows.  Reads  extracted
FinancialFacts  from  the  event  store  AND  historical  financials  from  the  Applicant  Registry.  The  LLM  identifies
pattern anomalies; Python computes the fraud_score from weighted anomalies.
## NODE KEY LOGIC
load_facts Load ExtractionCompleted events from docpkg-{id}. Get current-year FinancialFacts.
cross_reference_registry Load 3yr financial_history from Applicant Registry. Compute deltas: current vs prior year for
revenue, EBITDA, margins.
analyze_fraud_patterns
LLM  call:  "Given  extracted  current-year  figures  and  3-year  history,  identify  anomalous  gaps."
Returns  list  of  FraudAnomaly  objects  each  with  anomaly_type,  description,  severity,  evidence.
fraud_score  =  sum(severity_weights).  Score  >  0.60  →  DECLINE.  Score  0.30–0.60  →
## FLAG_FOR_REVIEW.
write_output Append FraudScreeningCompleted on fraud stream. Append ComplianceCheckRequested on
loan stream.

Agent 4 — ComplianceAgent (stub_agents.py)
Evaluates 6 regulatory rules in sequence. Rules are deterministic Python — no LLM in the decision path. LLM is
used only   in   the   write_output   node   to   generate   human-readable   evidence   summaries.   A   hard   block
(is_hard_block=True) stops rule evaluation immediately — no further rules are checked.
## RULE ID RULE NAME HARD
## BLOCK?
## WHAT TO CHECK
REG-001 Bank Secrecy Act Check No
## (remediabl
e)
company   has  no  compliance_flag  with   flag_type=AML_WATCH
AND is_active=True
## REG-
## 002
OFAC Sanctions
## Screening
YES company has no compliance_flag with
flag_type=SANCTIONS_REVIEW AND is_active=True
## REG-
## 003
## Jurisdiction Lending
## Eligibility
YES company.jurisdiction != "MT" (Montana excluded for this exercise)
## REG-
## 004
## Legal Entity Type
## Eligibility
## No
## (remediabl
e)
NOT (legal_type=="Sole Proprietor" AND requested_amount_usd >
## 250000)
## REG-
## 005
## Minimum Operating
## History
YES (2026 - company.founded_year) >= 2
## REG-
## 006
CRA Community
## Reinvestment Act
## No
## (informatio
nal)
Always  NOTED  (not  passed/failed).  Append  ComplianceRuleNoted
with note_type="CRA_CONSIDERATION".

# ComplianceAgent node pattern — deterministic rules, no LLM in decision path

TRP1  · The Ledger  ·  Challenge Document March 2026
Page 11  ·  Confidential  ·  TRP1 FDE Program

# build_graph() uses conditional edges to stop after hard block:
graph.add_node("evaluate_reg001", self._node_evaluate_reg001)
graph.add_node("evaluate_reg002", self._node_evaluate_reg002)
## # ...
graph.add_conditional_edges(
## "evaluate_reg001",
lambda s: "hard_block" if s.get("hard_block") else "evaluate_reg002",
## {"evaluate_reg002": "evaluate_reg002", "hard_block": "write_output"}
## )

# Each rule node follows this pattern:
async def _node_evaluate_reg003(self, state):
t0 = time.time()
company = state["company_profile"]
passes = company.jurisdiction != "MT"
await self._append_compliance_result(state, "REG-003", "Jurisdiction Check", passes,
is_hard=True)
await self._record_node_execution("evaluate_reg003", [...], [...], ms=...)
return {**state, "hard_block": not passes, "rules_evaluated":
state["rules_evaluated"] + 1}


Agent 5 — DecisionOrchestratorAgent (stub_agents.py)
The only agent that reads from other agents' output streams. Synthesises credit, fraud, and compliance results into
a final recommendation. Hard constraints enforced in Python after the LLM synthesises the executive summary.
The recommendation may be overridden by constraints; the summary always explains why.
## NODE READS FROM OUTPUT
load_credit credit-{id} stream:
CreditAnalysisCompleted
risk_tier, recommended_limit, confidence, rationale,
data_quality_caveats
load_fraud fraud-{id} stream:
FraudScreeningComplete
d
fraud_score, risk_level, anomalies_found, recommendation
load_compliance compliance-{id} stream:
ComplianceCheckComplet
ed
overall_verdict (CLEAR/BLOCKED/CONDITIONAL), has_hard_block
synthesize_decisi
on
All loaded analysis data LLM call: produce executive_summary (3–5 sentences), key_risks (list),
initial recommendation. Returns OrchestratorDecision JSON.
apply_hard_const
raints
OrchestratorDecision +
loaded data
Python  rules:  compliance  BLOCKED  →  force  DECLINE.  confidence  <
0.60 → force REFER. fraud_score > 0.60 → force REFER. Overrides LLM
recommendation if needed.
write_output Final decision DecisionGenerated on loan stream. ApplicationApproved or
ApplicationDeclined if auto. HumanReviewRequested if REFER.



TRP1  · The Ledger  ·  Challenge Document March 2026
Page 12  ·  Confidential  ·  TRP1 FDE Program
## SECTION 5  ·  TECHNOLOGY STACK

## COMPONENT PACKAGE VERSIO
## N
## WHY — NOT THE ALTERNATIVE
LangGraph    (agent
graphs)
langgraph >=0.2 Compiled StateGraph gives explicit node sequences, conditional edges,
and built-in async support. Do NOT use bare asyncio task loops — the
graph structure is what makes crash recovery possible.
LLM API anthropic >=0.30 Direct SDK. No LangChain wrappers. You need to see raw token counts
(response.usage) for cost attribution. Always use AsyncAnthropic for the
async node pattern.
Event store DB
driver
asyncpg >=0.29 Native  PostgreSQL  protocol.  Non-blocking.  Fastest  Python  Postgres
driver.  NOT  psycopg2  (blocking)  or  SQLAlchemy  ORM  (hides  the
schema).
## Event/agent
schemas
pydantic >=2.6 All  event  payloads,  agent  state,  registry  query  results  are  Pydantic  v2
BaseModel. Validation is not optional. Use model_dump(mode="json")
for serialisation.
MCP server fastmcp >=0.9 Decorator-based MCP server. @mcp.tool() for commands,
@mcp.resource() for projection queries.
PDF generation
## (datagen)
reportlab >=4.2 For  generating  the  160  financial  statement  PDFs.  Only  needed  in
datagen/.
PDF extraction
## (agents)
## Your  Week  3
pipeline
MinerU
or
## Docling
Reuse your Week 3 implementation exactly. DocumentProcessingAgent
wraps it.
## Excel
generation/parsing
openpyxl >=3.1 For  generating  .xlsx  files  and  for  DocumentProcessingAgent  to  parse
.xlsx uploads.
Fake data
generation
faker >=24 Company names, EINs, addresses. Used only in datagen/.
Testing pytest +
pytest-
asyncio
## >=8.0,
## >=0.23
asyncio_mode  =  auto  in  pytest.ini — all  async  tests  work  without
## @pytest.mark.asyncio.

# requirements.txt — pin everything
asyncpg>=0.29.0,<0.30
anthropic>=0.30.0,<0.40
pydantic>=2.6.0,<3.0
langgraph>=0.2.0,<0.3
fastmcp>=0.9.0,<1.0
reportlab>=4.2.0,<5.0
openpyxl>=3.1.0,<4.0
faker>=24.0.0,<25.0
python-dotenv>=1.0.0,<2.0
pytest>=8.0.0,<9.0
pytest-asyncio>=0.23.0,<0.24
# Your Week 3 deps (add whichever you used):
# mineru>=1.0  OR  docling>=2.0

# pytest.ini
## [pytest]
asyncio_mode = auto

# .env (never commit)
ANTHROPIC_API_KEY=sk-ant-...
DATABASE_URL=postgresql://localhost/apex_ledger
DOCUMENTS_DIR=./documents
## REGULATION_VERSION=2026-Q1
## LOG_LEVEL=INFO


TRP1  · The Ledger  ·  Challenge Document March 2026
Page 13  ·  Confidential  ·  TRP1 FDE Program

apex-ledger/
├── .env                          # Never committed
├── pytest.ini                    # asyncio_mode = auto
├── requirements.txt
├── DOMAIN_NOTES.md               # 6 required questions answered before Day 2
├── DESIGN.md                     # 6 required sections — completed Day 10
├── DATA_GENERATION.md            # Simulator rules, PDF variants, event counts, API
costs
## │
├── datagen/                      # Data generator (provided — do not modify core logic)
│   ├── generate_all.py           # Main entry point
│   ├── company_generator.py      # 80 companies with GAAP financials
│   ├── pdf_generator.py          # GAAP PDF income statement + balance sheet
│   ├── excel_generator.py        # Multi-sheet GAAP Excel workbook
│   ├── event_simulator.py        # Full agent event simulation for seeding
│   └── schema_validator.py       # Validates all events against EVENT_REGISTRY
## │
├── ledger/                       # Your application package
│   ├── schema/events.py          # Canonical event schema (provided — 45 event types)
│   ├── event_store.py            # EventStore (implement) + InMemoryEventStore
## (provided)
│   ├── upcasters.py              # UpcasterRegistry (provided + 2 upcasters to
implement)
│   ├── domain/aggregates/        # LoanApplicationAggregate (stub — implement apply())
│   ├── projections/              # ProjectionDaemon + 3 projection classes (stub)
│   ├── agents/
│   │   ├── base_agent.py         # BaseApexAgent (provided)
│   │   ├── credit_analysis_agent.py  # Reference implementation (provided)
│   │   └── stub_agents.py        # 4 agent stubs (implement)
│   ├── registry/client.py        # ApplicantRegistryClient (stub — implement)
│   └── mcp_server.py             # FastMCP server (stub — implement Phase 5)
## │
├── documents/                    # Generated files (gitignored, regenerable)
├── tests/
│   ├── conftest.py               # Fixtures using InMemoryEventStore
│   ├── phase1/test_event_store.py  # EventStore tests (10 provided — use
InMemoryEventStore)
│   ├── test_schema_and_generator.py  # Phase 0 schema tests (10 provided — all pass)
│   ├── test_event_store.py         # Real DB tests (skip until EventStore implemented)
│   ├── test_narratives.py          # 5 narrative tests (skipped — implement after
agents)
│   └── phase2-5/                   # Implement phase by phase
└── scripts/
├── run_pipeline.py             # Process one application end-to-end
└── demo_narr05.py              # Required demo script (Day 10)




TRP1  · The Ledger  ·  Challenge Document March 2026
Page 14  ·  Confidential  ·  TRP1 FDE Program
## SECTION 6  ·  TEN-DAY IMPLEMENTATION PLAN

Days 1–5 build the foundation: data generation, event store, and document processing. No LLM calls until Day 5.
Days 6–10 layer on the real agents, projections, and production quality. Day 10 is exclusively for DESIGN.md, demo,
and submission packaging.

## WEEK 1 — FOUNDATION

## DAY FOCUS DELIVERABLE BY END OF
## DAY
## GATE TEST
## Day
1Mon
Run the data
generator.Study the
seed events.Write
DOMAIN_NOTES.md.
80 companies in
applicant_registry   schema.160
PDFs  +  80  Excel  +  80  CSV  in
documents/.1,847   seed   events
in event
store.DOMAIN_NOTES.md: all
6 questions answered.
python     datagen/generate_all.py     exits     0.python
datagen/generate_all.py --validate-only: 0 errors.psql:
SELECT count(*) FROM events; → 1847.
## Day
2Tue
## Implement
EventStore:stream_versi
on()   →   append()   →
load_stream() →
load_all().All  in  a  single
transaction with OCC.
EventStore passes the  provided
real-DB test
suite.InMemoryEventStore
already  passes  phase1  tests —
confirm your real
implementation matches.
pytest     tests/test_event_store.py -v     (requires
DB).All 10 pass.Key:
test_concurrent_double_append_exactly_one_su
cceeds.
## Day
3Wed
## Implement
LoanApplicationAggreg
ate.apply()  for  all  event
types.Implement
ApplicantRegistryClien
t (4 query
methods).Implement
DocumentProcessingAg
ent (6 nodes).
DocumentProcessingAgent.pro
cess_application("APEX-
0007") runs end to end.Docpkg
stream has
ExtractionCompleted events
with    non-null    total_revenue
and
net_income.AgentNodeExecute
d events appear in agent session
stream.
python  scripts/run_pipeline.py --app APEX-0007
--phase   documentExtractionCompleted   event   in
docpkg
stream.QualityAssessmentCompleted.is_coherent
=   True.CreditAnalysisRequested   event   in   loan
stream.
## Day
4Thu
## Implement
CreditAnalysisAgent
(reference   provided —
study it
first).Implement
FraudDetectionAgent.
Credit  +  Fraud  run  on  APEX-
0012 through APEX-
0018.CreditAnalysisCompleted
events have non-empty
rationale and valid
confidence.FraudScreeningCo
mpleted events in fraud
streams.
python scripts/run_pipeline.py --app APEX-0012 -
-phase  creditCreditAnalysisCompleted.confidence
between 0.55 and
0.95.CreditAnalysisCompleted.decision.risk_tier
is     LOW/MEDIUM/HIGH.pytest     tests/phase2/
(implement these tests).
## Day
5Fri
## Implement
ComplianceAgent    and
DecisionOrchestratorA
gent.NARR-01 (OCC
collision) and NARR-04
(Montana     compliance
block) should now pass.
All  5  agents  working.At  least  5
applications  reach  APPROVED
or   DECLINED   state.NARR-01
and NARR-04 pass.
pytest tests/test_narratives.py::test_narr01
tests/test_narratives.py::test_narr04Both
pass.psql: SELECT state, count(*) FROM  ... (your
projection or direct query).

## WEEK 2 — AGENTS, PROJECTIONS, PRODUCTION QUALITY

## DAY FOCUS DELIVERABLE BY END OF
## DAY
## GATE TEST
## Day
6Mon
## Implement
ProjectionDaemon  and
all 3 projections.NARR-
All  3  projections  rebuild  from
seed events
correctly.ApplicationSummary
pytest
tests/test_narratives.py::test_narr02ApplicationS
ummary    projection    shows    8    APPROVED,    3

TRP1  · The Ledger  ·  Challenge Document March 2026
Page 15  ·  Confidential  ·  TRP1 FDE Program
## DAY FOCUS DELIVERABLE BY END OF
## DAY
## GATE TEST
## 02 (document
extraction   failure   with
missing EBITDA)
should pass.
shows  correct  counts  for  all  29
seeded    applications.NARR-02
passes.
## DECLINED,     1     DECLINED_COMPLIANCE,     1
REFERRED from seed data.
## Day
7Tue
Implement crash
recovery (NARR-
03).Implement
HumanReviewComplet
ed   command    handler
(NARR-05).Run load
generator — measure
projection lag.
All 5 narrative tests
passing.Load generator: 15
concurrent applications, 6
workers,    0    unresolved    OCC
collisions.Projection lag <
800ms under load.
pytest   tests/test_narratives.py — all  5  pass.python
load_gen/run_concurrent.py --applications   15 --
concurrency  6→  occ_collision_report.txt:  10–35
collisions, all resolved.
## Day
8Wed
Implement upcasters
(Phase    4).Build    audit
integrity chain
(AuditIntegrityCheckR
un with SHA-256
chain).Implement
AgentContextReconstru
ctor     for     Gas     Town
recovery.
UpcasterRegistry has both
required
upcasters.Immutability test
passes  (upcast  does  not  modify
DB row).Audit chain for APEX-
0021 verifiable independently.
pytest tests/phase4/Key:
test_upcaster_does_not_write_to_events_tablete
st_audit_chain_is_independently_verifiable
## Day
9Thu
Implement MCP server:
8 tools + 6
resources.Full   lifecycle
integration test via MCP
only (12 assertions).
MCP   server   running   on   port
8765.All  8  tools  callable.All  6
resources return projection
data.Full lifecycle test passes.
pytest
tests/phase5/test_full_lifecycle_via_mcp.py12
assertions pass.Key: application reaches
APPROVED state having used only MCP tools.
## Day
10Fri
DESIGN.md (all 6
sections).python
scripts/demo_narr05.p
y — runs    under    90
seconds.api_cost_repo
rt.txt
generated.Submission
folder packaged.
DESIGN.md
complete.demo_narr05.py runs
cleanly.regulatory_package_N
ARR05.json passes
verify_package.py.All   required
artifacts in artifacts/.
python tests/phase6/verify_package.py
artifacts/regulatory_package_NARR05.jsonpytest
tests/ -q → 0 failures (skipped DB tests excluded).

## If You
## Finish
## Early
Phase  6  bonus:  WhatIfProjector  (replay  NARR-05  event  history  with  substituted  credit
decision)  and  generate_regulatory_package()  (self-contained,  independently  verifiable
JSON audit package). Phase 6 is the Score 5 qualifier. Without it, maximum score on any
criterion  is  4.  Attempt  Phase  6  only  after  all  5  narrative  tests  pass  and  DESIGN.md  is
complete.



TRP1  · The Ledger  ·  Challenge Document March 2026
Page 16  ·  Confidential  ·  TRP1 FDE Program
## SECTION 7  ·  THE FIVE NARRATIVE SCENARIOS

These five applications are not in the seed data. You generate them by running your agents against companies from
the Applicant Registry. Each tests a production failure mode. The automated test harness checks the exact event
sequence. Passing all five is the primary correctness gate for the challenge.

NARR-01 — Concurrent OCC Collision
## FIELD SPECIFICATION
Company COMP-031 — manufacturing sector, MEDIUM risk, revenue ~$3.8M
Trigger Two  CreditAnalysisAgent  instances  are  started  simultaneously  on  NARR-01  after  documents  are
processed. Both read the credit stream at version 0 (CreditRecordOpened only).
## Expected
sequence
Agent A appends CreditAnalysisCompleted at expected_version=0 (succeeds → stream version becomes 1).
Agent B hits OptimisticConcurrencyError, reloads the stream, sees Agent A's result, confirms analysis is
still needed (it is — Agent A's result is for the same application), appends its own CreditAnalysisCompleted
at expected_version=1 (succeeds → version becomes 2). Both agents complete without raising to the caller.
Test assertions credit  stream  has  exactly  2  CreditAnalysisCompleted  events.  stream_position  1  and  2  both  have
event_type=CreditAnalysisCompleted.   No   unhandled   exceptions   in   agent   logs.   Second   event's
metadata["causation_id"] is resolvable.

NARR-02 — Document Extraction Failure (Missing EBITDA)
## FIELD SPECIFICATION
Company COMP-044 — healthcare  sector,  STABLE  trajectory,  income  statement  PDF  is  the  missing_ebitda
variant
Trigger DocumentProcessingAgent processes NARR-02 and encounters a PDF with no EBITDA line item.
## Expected
sequence
ExtractionCompleted  has  facts.ebitda=None  and  field_confidence["ebitda"]=0.0  and  "ebitda"  in
extraction_notes. QualityAssessmentCompleted has "ebitda" in critical_missing_fields.
CreditAnalysisAgent  receives  the  quality  flags,  notes  data_quality_caveats  in its  output,  and  caps
confidence at 0.75. Application continues — it is not blocked.
Test assertions ExtractionCompleted.payload.facts["ebitda"] is None.
QualityAssessmentCompleted.payload.critical_missing_fields contains "ebitda".
CreditAnalysisCompleted.payload.decision["confidence"] <= 0.75.
CreditAnalysisCompleted.payload.decision["data_quality_caveats"] is non-empty list.

NARR-03 — Agent Crash and Recovery
## FIELD SPECIFICATION
Company COMP-057 — technology sector, GROWTH trajectory, $1.1M requested
Trigger FraudDetectionAgent starts processing NARR-03 and crashes after the load_facts node (simulated in
test by calling agent._simulate_crash_after_node("load_facts")).
## Expected
sequence
AgentSessionFailed    event    in    the    crashed    session's    stream    with    recoverable=True    and
last_successful_node="load_facts". A new FraudDetectionAgent instance starts. Its
reconstruct_agent_context() reads the crashed session's stream and identifies load_facts completed.
New  session  starts  with  context_source="prior_session_replay:{crashed_session_id}".  Recovery
resumes from cross_reference_registry node (skipping load_facts). No duplicate load_facts work.
Test assertions Exactly ONE FraudScreeningCompleted in fraud stream. Second
AgentSessionStarted.context_source  starts   with   "prior_session_replay:".   AgentSessionRecovered
event  present  in  new  session  stream.  Zero  duplicate  AgentNodeExecuted  events  for  "load_facts"
across both sessions.


TRP1  · The Ledger  ·  Challenge Document March 2026
Page 17  ·  Confidential  ·  TRP1 FDE Program
NARR-04 — Compliance Hard Block (Montana)
## FIELD SPECIFICATION
Company The Montana company (jurisdiction="MT") — whichever COMP-ID the generator assigned
Trigger ComplianceAgent evaluates rules sequentially. REG-003 fails (Montana excluded).
## Expected
sequence
ComplianceRulePassed  for  REG-001.  ComplianceRulePassed  for  REG-002.  ComplianceRuleFailed
for    REG-003    with    is_hard_block=True.     No    further    rule    events — evaluation    stops.
ComplianceCheckCompleted with overall_verdict="BLOCKED". ApplicationDeclined on loan stream
with adverse_action_notice_required=True.
Test assertions compliance  stream  has  exactly  3  events:  2  Passed  +  1  Failed  (REG-004  through  REG-006  never
evaluated). NO DecisionGenerated event ever appears in the loan stream.
ApplicationDeclined.payload["decline_reasons"] contains a string matching "REG-003".
ApplicationDeclined.payload["adverse_action_notice_required"] is True.

NARR-05 — Human Override (The Loan Officer Approves Against the Agent)
## FIELD SPECIFICATION
Company COMP-068 — retail sector, 15-year bank customer, DECLINING revenue trajectory (−8% YoY), high leverage.
Prior loan repaid on schedule.
Trigger Full  pipeline  runs.  DecisionOrchestrator  recommends  DECLINE  (HIGH  risk,  low  confidence).  A
human loan officer overrides.
## Expected
sequence
DecisionGenerated with recommendation="DECLINE" and confidence=0.82.
HumanReviewRequested    on    loan    stream.    Human    override:    HumanReviewCompleted    with
override=True,  reviewer_id="LO-Sarah-Chen",  final_decision="APPROVE",  override_reason="15-
year    customer,    prior     repayment    history,    collateral    offered".     ApplicationApproved    with
approved_amount_usd=750000   (less   than   $950K   requested),   conditions=["Monthly   revenue
reporting for 12 months", "Personal guarantee from CEO"].
Test assertions DecisionGenerated.payload["recommendation"]=="DECLINE".
HumanReviewCompleted.payload["override"]==True.
HumanReviewCompleted.payload["reviewer_id"]=="LO-Sarah-Chen".
ApplicationApproved.payload["approved_amount_usd"]==750000.
ApplicationApproved.payload["conditions"]   has   len==2.   This   is   the   application   used   for   the
regulatory package demo.



TRP1  · The Ledger  ·  Challenge Document March 2026
Page 18  ·  Confidential  ·  TRP1 FDE Program
## SECTION 8  ·  SUBMISSION REQUIREMENTS

submission/
├── README.md              # Install → seed → run. Under 1 page. Must work from scratch.
├── requirements.txt       # Pinned. pip install -r requirements.txt must succeed.
├── .env.example           # Template with all var names. No real keys.
├── pytest.ini             # asyncio_mode = auto
├── DOMAIN_NOTES.md        # 6 questions. Graded separately.
├── DESIGN.md              # 6 sections. Graded separately.
├── DATA_GENERATION.md     # Simulator rules, PDF variants, event counts, API costs per
agent.
## │
├── datagen/               # Complete data generator (provided — include as-is)
## │
├── ledger/                # Your application package
│   ├── schema/events.py   # Canonical schema (include as-is)
│   ├── event_store.py     # Your implementation
│   ├── upcasters.py       # Your implementation
│   ├── domain/aggregates/loan_application.py  # Your implementation
│   ├── projections/       # Your implementation
│   ├── agents/            # All 5 agents
│   ├── registry/client.py # Your implementation
│   └── mcp_server.py      # Your implementation
## │
├── tests/                 # Full test suite
├── scripts/
│   ├── run_pipeline.py    # Process one application through all agents
│   └── demo_narr05.py     # Required demo — must run in < 90 seconds
## │
└── artifacts/             # Generated artifacts (commit these)
├── test_results.txt
├── narrative_test_results.txt
├── occ_collision_report.txt
├── projection_lag_report.txt
├── api_cost_report.txt
└── regulatory_package_NARR05.json


The api_cost_report.txt Requirement
Every  LLM  call  is  tagged  with  agent_type  and  workflow_id  via  the  Week  5  Sentinel  CostAttributor.  The
api_cost_report.txt is its output — generated by running your full pipeline on all 29 seed applications plus the 5
narrative applications. Costs over $50 total signal inefficient prompt design.
## REQUIRED FIELD EXAMPLE VALUE SIGNALS
Total  API  cost  for  all  34
applications
## $22.40
Above $50 → prompts too long. Below $8 → suspicious (may not
be calling real LLM).
Average cost per
application (range)
avg  $0.66,  range  $0.18–
## $1.40
High  variance  →  some  application  types  much  more  expensive.
Identify them.
Cost by agent (all 5) DocProc    $2.10        Credit
## $8.80  Fraud $4.20
## Compliance $0.00
## Orchestrator $7.30
Compliance   is   $0.00   (no   LLM   calls   in   rule   evaluation).
Orchestrator is often second-heaviest.
Most expensive single call APEX-0049 Credit
## Analysis:     $0.82    (5,200
input tokens)
Usually caused by very long historical financial context or many
quality caveats requiring long prompts.

DESIGN.md — The Six Required Sections

TRP1  · The Ledger  ·  Challenge Document March 2026
Page 19  ·  Confidential  ·  TRP1 FDE Program
## SECTION MINIMUM CONTENT WHAT IS GRADED
## 1. Data
## Boundary
## Decisions
For  each  of  the  7  data  types  in
Section  1:  why  it  lives  where  it
does. Specifically: why are
compliance_flags in the  Applicant
Registry and not in the event store?
Depth  of  reasoning,  not  correctness.  A  wrong  answer  with  good
reasoning scores higher than a right answer with no reasoning.
## 2. Aggregate
## Boundary
## Justification
For each of the 6 aggregate types:
why this stream boundary. Include
one alternative you considered and
why  you  rejected  it.  Include  the
OCC implication of your boundary
choice.
Concurrency analysis. "I chose this boundary because  agents can
work concurrently without stepping on each other" is the right kind
of answer.
## 3. Week 3
## Integration
## Architecture
The exact contract between
DocumentProcessingAgent and
the   Week   3   pipeline.   What   is
passed in. What comes back. What
happens    if    the    pipeline    fails
partially  (some  fields  None,  some
populated).
Specifically:  how  do  ExtractionCompleted  events  handle  partial
extraction? How does CreditAnalysisAgent respond to
data_quality_caveats?
- LangGraph
## Prompt   Design
## —
CreditAnalysis
## Agent
What   is   in   the   system   prompt.
What is in the user message. What
is  in neither (and why). What you
tried that you then changed — with
the  before/after  prompt  and  the
reason for the change.
The  "what I  tried and changed" section is the  most  important. It
demonstrates    iterative    prompt    engineering    discipline.    One
iteration is minimum; three is good.
## 5. Agent Failure
Modes and
## Recovery
For  each  agent:  the  failure  mode
(LLM timeout, non-parseable
JSON  response,  OCC  max-retries
exceeded,  registry  query  failure),
how   the   system   recovers,   what
events  are  produced.  Must  cover
NARR-03 crash recovery in detail.
Completeness.  "The  LLM  always  returns  valid  JSON"  is  not  an
acceptable answer.
## 6. What I
## Would Do
## Differently
One    architectural    decision    per
week  (two  total)  that  you  would
change  given  another  full  week.
Specific, with the tradeoff analysis.
Honesty and engineering judgment. The most senior signal in the
document. Vague answers score 1.



TRP1  · The Ledger  ·  Challenge Document March 2026
Page 20  ·  Confidential  ·  TRP1 FDE Program
## SECTION 9  ·  ASSESSMENT RUBRIC

Score 3: system works end-to-end on the happy path. Score 4: failure modes handled correctly; DESIGN.md shows
genuine  reasoning.  Score  5:  all  5  narrative  scenarios  pass;  projections  within  SLO;  real  prompt  engineering
evidence; Phase 6 attempted.

## CRITERION 1 2 3 4 5
Event Store + OCC
No working store append() works;
no concurrency
control
OCC enforced;
concurrent
double-append
test passes;
load_stream() and
load_all() work
All methods;
outbox in same
transaction; load
test passes with 0
unresolved OCC
collisions
## Above +
DESIGN.md
justifies schema;
retry strategy
quantified from
load generator
data
## Document Pipeline
## Integration
Week 3 not
connected
Files processed;
no
ExtractionComple
ted events
ExtractionComple
ted with
FinancialFacts;
QualityAssessmen
tCompleted;
PackageReadyFor
Analysis triggers
next agent
## NARR-02
(missing EBITDA)
handled
gracefully;
data_quality_cave
ats appear in
credit decision
## Above +
ExtractionFailed
handled; credit
agent adjusts
confidence for
low-quality fields;
all 4 PDF variants
exercised
LangGraph Agent Quality No LangGraph; or
no real LLM calls
LLM called;
output not
validated by
Pydantic; no
AgentNodeExecut
ed events
All 5 agents: LLM
called, Pydantic
output validation,
AgentNodeExecut
ed per node,
correct next-agent
trigger
DESIGN.md
Section 4 shows
prompt iteration
evidence;
confidence and
rationale fields
substantive;
NARR-03 crash
recovery
Above + cost
report shows
## <$0.70/applicatio
n avg; Gas Town
crash recovery
works for all 5
agents
## Business Rules +
## Narratives
No domain logic Some rules; no
narrative tests
All 6 aggregate
rules enforced;
## NARR-01 (OCC)
and NARR-04
## (compliance
block) pass
All 5 narrative
tests pass
All 5 pass + load
test: 0 unresolved
OCC collisions;
## NARR-05
regulatory package
independently
verifiable
Projections + Daemon No projections One projection; no
lag metric; no
checkpointing
All 3 projections;
daemon with
checkpointing; lag
metric exposed
SLO met under
load (<800ms);
temporal query on
ComplianceAudit
View; rebuild-
from-zero tested
## Above +
projection_lag_re
port.txt artifact;
rebuilds
confirmed correct
against seed
events
MCP Server No MCP server Tools callable; no
structured errors;
no resources
All 8 tools + 6
resources; full
lifecycle
integration test
(12 assertions)
passes
Resources read
only from
projections;
structured error
types; MCP cost
attribution
working
## Above +
api_cost_report
shows MCP tool
cost breakdown
correctly
attributed
Data Architecture Quality Generator fails or
skips
Seed events
present; no
registry; agents
use hardcoded
data
Generator clean;
registry seeded;
agents query
registry read-only;
## DATA_GENERAT
ION.md present
All PDF variants
exercised; event
count verified;
simulator-vs-real
event shape
compared in
## DATA_GENERAT
ION.md
## Above + 34+
applications
processed through
full pipeline; event
store has >3,000
events; cost report
complete
DESIGN.md Depth
Not present Describes what
was built (no
reasoning)
All 6 sections
present with basic
reasoning
## Aggregate
boundary
justification
includes OCC
analysis; LLM
section shows at
DESIGN.md
useful to a new
engineer joining 6
months later;
"what I would do
differently" is

TRP1  · The Ledger  ·  Challenge Document March 2026
Page 21  ·  Confidential  ·  TRP1 FDE Program
## CRITERION 1 2 3 4 5
least one prompt
iteration
honest and
specific

## Phase 6 Bonus — Score 5 Qualifier
Attempt only after all 5 narrative scenarios pass and DESIGN.md is complete. Without Phase 6, maximum score is
4 on any criterion.
## DELIVERABLE WHAT IT IS GATE TEST ARTIFACT
WhatIfProjecto
r
Replays   NARR-05   event   history
with  a  substituted  credit  decision
(MEDIUM  risk  instead  of  HIGH
risk).   Filters   causally   dependent
events. Shows whether the
orchestrator would have approved
without the human override.
python scripts/run_whatif.py --
application  APEX-NARR05 --
substitute-credit-tier
MEDIUM→ Different
recommendation than real
timeline. Causal filter verified by
test.
artifacts/counterfactual_narr05.
json
generate_regul
atory_package(
## )
Produces  a  self-contained  JSON
package for NARR-05: all events in
order,    all    projection    states    at
examination_date,     audit     chain
integrity    proof,    model    version
provenance, plain-English
narrative    suitable    for    a    bank
regulator.
python
tests/phase6/verify_package.py
artifacts/regulatory_package_N
ARR05.json→ Hash chain valid.
All    events    present.    Package
independently verifiable
without accessing the live event
store.
artifacts/regulatory_package_N
ARR05.json



TRP1 FDE Program  ·  W eek 5: The Ledger v3  ·  March 2026  ·  Confidential