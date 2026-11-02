# Product Requirements Document: HR Assist — AI Resume Screening Agent

## 1. Problem Statement

HR Recruiters spend excessive time manually screening resumes one-by-one from an application database. The current process suffers from five core pain points:

| # | Pain Point | Impact |
|---|-----------|--------|
| 1 | **Slow screening time** | Recruiters review resumes sequentially; high-volume hiring creates bottlenecks |
| 2 | **Poor job-description matching** | Unstructured resume data is hard to parse against job requirements |
| 3 | **Unstructured resume data** | Resumes arrive in varied formats (text, HTML) with no consistent schema |
| 4 | **Hard to verify candidate claims** | Especially skills proficiency — recruiters cannot efficiently assess whether claimed skills are credible |
| 5 | **Inconsistent candidate evaluation** | Manual screening produces subjective, non-reproducible assessments across recruiters and candidates |

## 2. Target Users

**Primary:** HR Recruiters
- Screen resumes submitted through the application website
- Match candidates to job postings across 25+ functional categories
- Make shortlist / reject decisions

## 3. Use Cases

### UC-1: Resume Data Ingestion & Parsing (One-time Setup)
- **Actor:** System (automated batch process)
- **Trigger:** Manual execution of ingestion script before deployment
- **Action:** 
  1. Read `Resume.csv` (66K records)
  2. Clean and parse each resume: extract structured fields (name, contact, summary, experience, education, skills)
  3. Store parsed data in PostgreSQL as JSONB
  4. Generate embeddings and store in Qdrant for semantic search
  5. Save HTML versions as static files (optional: GitHub Pages or local storage)
- **Output:** Fully indexed candidate database ready for screening

### UC-2: Resume Data Access (Runtime)
- **Actor:** Agent (automated queries)
- **Context:** All candidate resumes from `Resume.csv` (~66,000 records) are already ingested into PostgreSQL and Qdrant
- **Action:** Agent queries structured resume data and vector embeddings for screening
- **Output:** Ready access to parsed candidate records for matching and evaluation

### UC-2: Job-Description–Aware Candidate Matching
- **Actor:** HR Recruiter
- **Trigger:** Recruiter inputs or selects a job description with required skills, experience, and qualifications
- **Action:** Agent retrieves relevant resumes from vector store, scores each against JD criteria, ranks candidates
- **Output:** Ranked list of candidates with match scores and per-criteria breakdown

### UC-3: Skills Proficiency Verification
- **Actor:** HR Recruiter / Agent
- **Trigger:** Candidate claims a skill on resume
- **Action:** Agent cross-references claimed skills against work experience descriptions, project details, and certifications to assess credibility; flags inconsistencies
- **Output:** Skills verification report with confidence levels (verified / partially-supported / unverified)

### UC-4: Consistent Candidate Evaluation
- **Actor:** HR Recruiter
- **Trigger:** Recruiter reviews a candidate
- **Action:** Agent applies a standardized scoring rubric across all candidates for a given role (skills match, experience relevance, education fit, career trajectory)
- **Output:** Uniform evaluation scorecard per candidate

### UC-5: Batch Screening & Shortlisting
- **Actor:** HR Recruiter
- **Trigger:** Recruiter initiates screening for a job posting
- **Action:** Agent processes all submitted resumes for that posting, applies matching + evaluation, returns shortlist
- **Output:** Shortlisted candidates with evaluation summaries; rejected candidates with reason codes

## 4. Functional Requirements

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-1 | Access pre-parsed resume data from PostgreSQL (structured JSON) | P0 |
| FR-2 | Retrieve resume embeddings from Qdrant for semantic search | P0 |
| FR-3 | Accept job descriptions as input and extract key requirements | P0 |
| FR-4 | Rank candidates by semantic similarity to JD requirements | P0 |
| FR-5 | Generate per-candidate evaluation scorecards with standardized rubric | P0 |
| FR-6 | Verify skills claims against experience evidence | P1 |
| FR-7 | Provide Streamlit UI for recruiters to input JD, view results, and manage shortlists | P0 |
| FR-8 | Trace all agent decisions via Langfuse for auditability | P1 |
| FR-9 | Support 25+ job categories from the dataset | P0 |
| FR-10 | Filter candidates by category, skills, and experience level | P0 |
| FR-11 | Parse resume text and HTML to extract structured fields (name, contact, summary, experience, education, skills) | P0 |
| FR-12 | Clean and normalize resume data (remove formatting artifacts, standardize date formats) | P0 |
| FR-13 | Store parsed resume data in PostgreSQL as JSONB | P0 |
| FR-14 | Generate vector embeddings for semantic search and store in Qdrant | P0 |
| FR-15 | Save HTML resume versions as static files for archival/viewing | P1 |
| FR-16 | Handle parsing failures gracefully (log errors, flag for manual review) | P0 |
| FR-17 | Support batch processing with progress tracking and resumability | P0 |
| FR-18 | Use a supervisor-workers agent architecture for screening runs | P0 |
| FR-19 | Stream supervisor and worker progress to the Streamlit UI | P0 |
| FR-20 | Show worker failures, partial results, and retry state explicitly | P0 |
| FR-21 | Preserve an ordered event history for reconnecting to an active run | P1 |

## 5. Non-Functional Requirements

| ID | Requirement |
|----|-------------|
| NFR-1 | Screening a batch of 100 resumes against a JD completes within 5 minutes |
| NFR-2 | System handles the full dataset (~66,000 resumes) without degradation |
| NFR-3 | All PII (names, contact info) handled in compliance with data privacy standards |
| NFR-4 | Agent decisions are fully traceable via Langfuse logs |
| NFR-5 | UI is usable by non-technical HR recruiters with minimal training |
| NFR-6 | Every screening run exposes supervisor delegation and worker progress in near real time |
| NFR-7 | A worker failure never appears as a completed or fully reliable shortlist |

## 6. Dataset

- **Source:** `workload/ai-agent-streamlit-app/Resume.csv`
- **Size:** ~66,000 resumes
- **Columns:** `ID`, `Resume_str` (plain text), `Resume_html` (HTML), `Category`
- **Categories (25+):** Information Technology, Finance, Engineering, HR, Sales, Healthcare, Consulting, Banking, Construction, Design, Arts, Teaching, Aviation, Chef, Advocate, Fitness, Digital Media, Apparel, Business Development, Public Relations, and more
- **Resume structure:** Summary, Highlights, Experience (with dates, company, title, responsibilities), Education (degree, institution, year), Skills

## 7. Success Metrics

| Metric | Baseline (Current) | Target | Measurement |
|--------|-------------------|--------|-------------|
| **Screening time** | Manual, hours per posting | < 5 min for 100 resumes | Avg time from JD input to shortlist delivery |
| **Match accuracy** | Subjective, unknown | > 85% recruiter agreement on top-10 picks | Recruiter review of agent-ranked shortlists |
| **Evaluation consistency** | Low (inter-recruiter variance) | > 90% score stability | Same candidate scored across runs within ±5% |
| **Bad-hire reduction** | Unknown baseline | 20% reduction in first-year attrition | Track hires from agent-screened pool vs. manual |
| **Recruiter satisfaction** | N/A | NPS > 40 | Post-deployment survey |

## 8. Out of Scope (v1)

- Direct integration with external ATS systems (e.g., Greenhouse, Lever)
- Automated candidate outreach or scheduling
- Video interview analysis
- Background check automation
- Multi-language resume support (English only for v1)

## 9. Assumptions

- **Standalone tool:** No integration with external ATS systems; recruiters use this system independently
- **Pre-loaded dataset:** All resumes from `Resume.csv` are already ingested; no real-time ingestion pipeline needed for v1
- **Data scope:** System works exclusively with the 66K resumes in the dataset; no external resume sources
- Job descriptions are provided in text format by the recruiter
- Resume quality is sufficient for parsing (no scanned images without OCR)
- LLM API access is available and budgeted for inference costs
- Recruiter workflow remains human-in-the-loop (agent recommends, recruiter decides)
- **Unknown baselines:** Current manual screening time and accuracy are not measured; success metrics will be established post-deployment

## 10. Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| LLM hallucination in skills verification | Medium | High | Cross-reference with evidence; Langfuse tracing; human review |
| Resume parsing failures on unusual formats | Medium | Medium | HTML + text dual-input; fallback to manual flagging |
| Vector search returns irrelevant candidates | Medium | High | Hybrid search (semantic + keyword); tune top-k and thresholds |
| PII data leakage | Low | Critical | Access controls; audit logs; no PII in vector embeddings |
| LLM cost overrun at scale | Medium | Medium | Batch processing; caching; tiered model usage |
