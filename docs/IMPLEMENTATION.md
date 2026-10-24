# HR Assist Development Plan

This document describes the implementation sequence for the architecture in
[ARCHITECTURE.md](./ARCHITECTURE.md). It is intentionally separate from the
architecture document: the architecture describes the system design, while
this document tracks delivery order, milestones, and deferred work.

The project is planned for five working days. Each phase maps to one day and
must end with its exit criteria validated before the next phase begins.

## Development principles

- Build and validate one vertical slice before scaling to the full dataset.
- Keep the supervisor responsible for orchestration and workers responsible for
  one bounded capability.
- Stream every meaningful supervisor and worker state transition to the UI.
- Keep recruiter decisions human-in-the-loop; agent output is recommendation
  and evidence, not an automatic hiring decision.
- Do not expose PII in third-party traces or application logs.

## Day 1: Infrastructure and ingestion foundation

**Goal:** Run PostgreSQL and Qdrant locally and verify the first 100 resumes.

- [ ] Start PostgreSQL and Qdrant with Docker Compose.
- [ ] Create the PostgreSQL `resumes` table with JSONB `parsed_data`.
- [ ] Create the Qdrant `resumes` collection with 1536-dimensional vectors.
- [ ] Build regex extraction for emails, phones, URLs, and dates.
- [ ] Build the structured resume parser using the `ParsedResume` model.
- [ ] Process the first 100 resumes and spot-check field quality.
- [ ] Add checkpointing and explicit error logging.

**Exit criteria:**

- 100 resumes are parsed with at least 90% field coverage.
- PostgreSQL and Qdrant are reachable and contain matching sample records.
- Failed records are listed for manual review.

## Day 2: Full dataset ingestion

**Goal:** Index all resumes in PostgreSQL and Qdrant with resumable processing.

- [ ] Scale ingestion to the complete dataset using bounded batches.
- [ ] Normalize dates, skill names, whitespace, and HTML artifacts.
- [ ] Verify record and vector counts against the source dataset.
- [ ] Spot-check 100 random resumes for field coverage and correctness.
- [ ] Test a representative search such as `Python developer`.
- [ ] Save processed, failed, and average processing-time statistics.

**Exit criteria:**

- All valid source records are represented in PostgreSQL and Qdrant.
- The ingestion job can resume after interruption without duplicating records.
- Failed records and their reasons are available for review.

## Day 3: Supervisor and worker orchestration

**Goal:** Replace the single-step query flow with the supervisor-workers
workflow defined in the architecture.

- [ ] Define the run state and ordered streaming event contract.
- [ ] Implement the supervisor lifecycle: validate, delegate, aggregate, and
  complete or fail a run.
- [ ] Implement the requirements worker.
- [ ] Implement the retrieval worker using Qdrant and PostgreSQL.
- [ ] Implement the evaluation worker with evidence-backed scorecards.
- [ ] Implement the synthesis worker for ranked shortlist generation.
- [ ] Add bounded retries for retryable worker failures.
- [ ] Ensure dependent workers stop when a non-retryable prerequisite fails.

**Exit criteria:**

- A screening run produces ordered events for every worker.
- A failed worker produces an explicit failure event and never produces a
  success-shaped final result.
- The supervisor can return a complete shortlist with source evidence.

## Day 4: Streamlit streaming experience

**Goal:** Show supervisor and worker activity in the recruiter UI as it occurs.

- [ ] Add a live agent timeline showing delegation and worker status.
- [ ] Show progress counts and elapsed time for active workers.
- [ ] Stream scorecards into the results area as evaluations complete.
- [ ] Display uncertainty, missing evidence, and partial-result states clearly.
- [ ] Disable duplicate submissions while a run is active.
- [ ] Support reconnecting from the last received event sequence.
- [ ] Add empty-input, empty-result, timeout, and failure states.

**Exit criteria:**

- Recruiters can see which worker is active without waiting for the final
  response.
- Partial results are visibly distinct from a completed shortlist.
- A reconnecting UI can continue from stored event history.

## Day 5: Observability, testing, and portfolio polish

**Goal:** Produce a stable demo that is measurable and reviewable.

- [ ] Add Langfuse traces for supervisor and worker runs.
- [ ] Record latency, token usage, retrieved candidate IDs, and run status.
- [ ] Exclude names, contact details, and raw resume text from third-party logs.
- [ ] Test screening across technical, management, and entry-level roles.
- [ ] Measure P50/P95 latency and validate the target response time.
- [ ] Test empty results, malformed input, long responses, and special
  characters.
- [ ] Prepare a demo script with representative recruiter workflows.
- [ ] Remove temporary data and verify final ingestion statistics.

**Exit criteria:**

- The end-to-end flow is traceable by `run_id`.
- Critical failure and reconnect scenarios are covered by tests.
- The demo meets the agreed latency and shortlist-quality targets.

## Deferred to v2

| Feature | Reason |
|---------|--------|
| Deeper skills verification | Requires broader evidence cross-referencing and calibration |
| Multi-job-description batch screening | Single-job screening is the initial recruiter workflow |
| Admin analytics dashboard | Langfuse provides initial operational visibility |
| Cloud deployment | Local development and demo are sufficient for v1 |
| Comprehensive evaluation suite | Expand after the first recruiter feedback cycle |
| HTML/PDF resume archive viewing | Not required for the initial screening workflow |
