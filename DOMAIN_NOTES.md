# DOMAIN_NOTES.md — The Ledger (Weeks 9-10)

## Section 1: Conceptual Foundations

### QUESTION 1: EDA vs ES Distinction
**"A component uses callbacks (like LangChain traces) to capture event-like data. Is this EDA or ES? If redesigned using The Ledger, what changes and what is gained?"**

**Answer:**
Callback-based tracing is **Event-Driven Architecture (EDA)**, not Event Sourcing. In EDA, events are primarily *notifications* or *side-effects*. Crucially, these events **can be dropped** without losing the system's "source of truth" (the current state in a database). If the tracer fails or the network blips, the application state remains intact, but the history is lost.

In a **The Ledger (ES)** redesign:
- **What Changes**: 
    - Callbacks like `on_llm_start` are replaced by **`EventStore.append()`** calls that are part of the primary business transaction.
    - Trace data is transformed from ephemeral logs into **Typed Domain Events** (e.g., `AgentNodeExecuted`) stored in a persistent stream.
    - In-memory state (like a LangGraph state dict) is replaced by **Aggregate Reconstruction** via event replay; the aggregate is the only way to "know" the current state.
- **Specific Gains**:
    1. **Playability**: Every decision is fully **replayable**. We can reconstruct exactly what the Agent "knew" at any step by replaying the session's event stream.
    2. **Temporal Queries**: Compliance can **reconstruct state at any past timestamp**. We can ask "What was the credit limit of this application on 2024-03-12?" and get a deterministic answer by replaying events up to that point.
    3. **No Memory Loss**: This solves the "Gas Town" pattern (ephemeral memory loss). Since state is replayed from the store, a crash or restart never loses progress — the session resumes exactly where it left off.

### QUESTION 2: Aggregate Boundary Justification
**"Why are LoanApplication and AgentSession separate aggregates? Name one alternative boundary and trace the coupling it would introduce to a specific failure mode."**

**Answer:**
Aggregates are boundaries of **consistency and concurrency**. `LoanApplication` and `AgentSession` are separated to prevent write contention during the multi-agent orchestration phase.

- **The Alternative Boundary**: A "Fat" `LoanApplication` aggregate that contains all agent session nodes, tool calls, and LLM traces in a single stream (`loan-{id}`).
- **Coupling & Failure Mode**:
    - Under this merged boundary, every agent (Credit, Fraud, Compliance) working on the same application would **contend for the same stream position**.
    - If `CreditAgent` and `FraudAgent` both attempt to record a node execution simultaneously, both read version `N`. One succeeds (v`N+1`), the other fails with an **`OptimisticConcurrencyError`**.
    - **Scale Impact**: At 4 agents per application and 100 concurrent applications, this creates ~300 OCC collisions per minute. By separating them into `agent-{type}-{session}` and `loan-{id}`, agents can write their internal traces to their private streams with **zero contention**, only synchronizing on the `loan-{id}` stream when they are truly finished (the point of consistency).

---

## Section 2: Operational Mechanics

### QUESTION 3: Concurrency Control
**"Two AI agents simultaneously process the same loan application and both call append with expected_version=3. Trace the exact sequence."**

**Answer:**
1. **Initial State**: Both agents read the `loan-{id}` stream and see the current version is `3`.
2. **Race Starts**: Both call `append(expected_version=3)`.
3. **Agent 1 (Winner)**:
    - Reaches the DB first. Executes `SELECT current_version FROM event_streams WHERE stream_id = 'loan-X' FOR UPDATE`.
    - **DB-Level Row Lock** acquired. `current_version` is found to be `3`.
    - `3 == 3` (expected), so the write proceeds.
    - `INSERT` event(s) at `stream_position=4`.
    - `UPDATE event_streams SET current_version=4`.
    - `COMMIT` transaction; row lock released.
4. **Agent 2 (Loser)**:
    - Its `SELECT ... FOR UPDATE` was blocked until Agent 1's commit.
    - Now it executes and sees `current_version=4`.
    - `4 != 3` (late arrival).
    - The transaction **ROLLBACKS** immediately.
    - The `EventStore` raises **`OptimisticConcurrencyError(stream_id='loan-X', expected_version=3, actual_version=4)`**.
5. **Recovery**: The losing agent **reloads the stream** (re-reads to version 4). It inspects the new event at position 4 to see if its action is still relevant. If the winner was a competing decision, the loser may **ABANDON** its work. If the winner was an unrelated document upload, the loser **RETRIES** the append with `expected_version=4`.

### QUESTION 4: Projection Lag
**"Projection has 200ms typical lag. Loan officer queries available credit limit immediately after agent commits. They see the old limit. What does the system do?"**

**Answer:**
1. **Communication**: Sub-500ms lag is an **accepted operating condition** of our eventual consistency model. It is not an error, but a design trade-off for high write-availability.
2. **UI Implementation**: 
    - The system should display a **lag indicator** or timestamp: "Data as of [timestamp]. Refreshing...". This manages the user's perception of "state".
    - If the UI requires "Read-Your-Writes" consistency, it can poll the projection until the `global_position` exceeds the known event ID's position.
3. **Consistency Fallback**: For critical financial decisions where "close enough" is not good enough, the system provides a **Strong Consistency Fallback**. The query service can bypass the projection and **read directly from the aggregate event stream** to compute the limit on-the-fly, sacrificing latency for absolute correctness.

---

## Section 3: Advanced Patterns

### QUESTION 5: Upcasting
**"CreditAnalysisCompleted v1 has {application_id, decision, reason}. v2 needs {application_id, decision, reason, model_version, confidence_score, regulatory_basis}. Write the upcaster with field-level inference strategy."**

**Answer:**
```python
def upcast_credit_analysis_v1_to_v2(payload: dict) -> dict:
    return {
        **payload,
        "confidence_score": None,   # Unknown — cannot be fabricated
        "model_version": infer_model_version(payload.get("recorded_at")),  # Approximate
        "regulatory_basis": infer_regulatory_basis(payload.get("recorded_at")), # From timeline
    }
```
**Reasoning**:
- **`confidence_score` → NULL**: Fabrication is a compliance violation. If we "guess" 0.85, a compliance auditor would treat it as a real measurement. **Null signals genuine absence**; fabrication signals false precision.
- **`model_version` → Inferred**: We map the `recorded_at` timestamp against our deployment log. "Events between 2024-01 and 2024-03 were processed by `credit-1.2`." It is tagged as "(inferred)" to maintain audit honesty.
- **`regulatory_basis` → Inferred**: Similarly derived from the regulation version active at the time of the event's recording.

### QUESTION 6: Distributed Projection Coordination
**"How would you achieve distributed projection execution in Python? What coordination primitive and what failure mode?"**

**Answer:**
- **Primitive**: Use **PostgreSQL Advisory Locks** (`pg_advisory_lock(hash(projection_name))`). This is session-scoped and lightweight. Alternatively, use a Redis-based leader election with a TTL.
- **Failure Mode**: Without coordination, two daemon nodes would process the same batch of events, resulting in **Duplicate Writes** (double-counting metrics, etc.) which corrupts the projection data.
- **Recovery**: When a leader crashes, the advisory lock is automatically released by PostgreSQL (heartbeat failure). A follower acquires the lock and **resumes from the last checkpoint** (`projection_checkpoints.last_position`), ensuring exactly-once processing (provided the projection update and checkpoint update are atomic).
