import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import asyncio
import streamlit as st

from backend.resume_ingestion.internal.domain.core.resume.service.langgraph.init import (
    LanggraphResumeService
)
from backend.resume_ingestion.internal.registry.init import DomainRegistry

RESUME_DATASET_PATH = Path(__file__).resolve().parent / "Resume.csv"
REQUIRED_SECRETS = ("OPENROUTER_API_KEY", "OPENROUTER_API_BASE")


async def ingest() -> None:
    registry = DomainRegistry()
    service = registry.get("service.resume.langgraph")
    if not service:
        raise RuntimeError("LanggraphResumeService is not registered in the DomainRegistry")
    if isinstance(service, LanggraphResumeService):
        events = service.ingest_resume_records_to_knowledge_base(
            provider="local",
            file_path=str(RESUME_DATASET_PATH),
        )

        async for event in events:
            st.json(event)


st.set_page_config(page_title="Resume Ingestion")
st.title("Resume Ingestion")
st.caption("Ingests Resume.csv into the knowledge base via the resume ingestion workflow.")

if not RESUME_DATASET_PATH.is_file():
    st.error(f"Resume dataset not found: {RESUME_DATASET_PATH}")
    st.stop()

missing_secrets = [key for key in REQUIRED_SECRETS if key not in st.secrets]
if missing_secrets:
    st.error(f"Missing secrets: {', '.join(missing_secrets)}. Add them to .streamlit/secrets.toml.")
    st.stop()

if st.button("Run ingestion", type="primary"):
    with st.status("Ingesting resumes...", expanded=True) as status:
        try:
            asyncio.run(ingest())
            status.update(label="Ingestion complete", state="complete", expanded=False)
        except Exception as error:
            status.update(label="Ingestion failed", state="error", expanded=True)
            st.exception(error)
