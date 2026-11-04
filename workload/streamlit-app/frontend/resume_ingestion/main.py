import asyncio
import hashlib
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import streamlit as st
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.init import (
    LanggraphResumeService,
)
from backend.resume_ingestion.internal.pkg.config.config_database_sql import (
    database_sql_connection_string,
)
from backend.resume_ingestion.internal.pkg.constant.dialect import Dialect
from backend.resume_ingestion.internal.domain.model.database.sql.sqlalchemy.base import (
    Base,
)
from backend.resume_ingestion.internal.registry.init import DomainRegistry

UPLOAD_DIRECTORY = Path(__file__).resolve().parent / "uploads"
REQUIRED_SECRETS = (
    "OPENROUTER_API_KEY",
    "OPENROUTER_API_BASE",
    "PRIMARY_POSTGRES_DATABASE_SQL_URL",
    "PRIMARY_QDRANT_DATABASE_VECTOR_URL",
)


@st.cache_resource
def initialize_database() -> None:
    asyncio.run(
        _initialize_database(database_sql_connection_string(Dialect.POSTGRES))
    )


async def _initialize_database(connection_string: str) -> None:
    async with AsyncPostgresSaver.from_conn_string(connection_string) as checkpointer:
        await checkpointer.setup()

    engine = create_async_engine(
        connection_string.replace("postgresql://", "postgresql+asyncpg://", 1)
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()


async def _get_run_status(
    connection_string: str,
    provider: str,
    file_path: str,
) -> dict[str, Any] | None:
    engine = create_async_engine(
        connection_string.replace("postgresql://", "postgresql+asyncpg://", 1)
    )
    async with engine.begin() as connection:
        result = await connection.execute(
            text("""
                SELECT
                    source.xid,
                    run.xid,
                    run.status,
                    run.error,
                    run.last_processed_record_id,
                    run.progress_stage,
                    run.progress_value,
                    run.progress_message,
                    run.updated_at
                FROM ingestion_sources AS source
                LEFT JOIN ingestion_runs AS run
                    ON run.ingestion_source_id = source.id
                    AND run.is_current
                WHERE source.provider = :provider AND source.file_path = :file_path
            """),
            {"provider": provider, "file_path": file_path},
        )
        row = result.fetchone()
    await engine.dispose()
    if row is None:
        return None
    return {
        "source_xid": row[0],
        "run_xid": row[1],
        "status": row[2],
        "error": row[3],
        "last_processed_record_id": row[4],
        "progress_stage": row[5],
        "progress_value": row[6],
        "progress_message": row[7],
        "updated_at": row[8],
    }


def _status(provider: str, file_path: str) -> dict[str, Any] | None:
    return asyncio.run(
        _get_run_status(
            database_sql_connection_string(Dialect.POSTGRES),
            provider,
            file_path,
        )
    )


async def _get_ingestion_sources(
    connection_string: str,
) -> list[dict[str, Any]]:
    engine = create_async_engine(
        connection_string.replace("postgresql://", "postgresql+asyncpg://", 1)
    )
    async with engine.begin() as connection:
        result = await connection.execute(
            text("""
                SELECT
                    source.xid,
                    source.provider,
                    source.file_path,
                    COUNT(run.id) AS run_count,
                    MAX(run.updated_at) AS last_updated_at,
                    current_run.status AS current_status
                FROM ingestion_sources AS source
                LEFT JOIN ingestion_runs AS run
                    ON run.ingestion_source_id = source.id
                LEFT JOIN ingestion_runs AS current_run
                    ON current_run.ingestion_source_id = source.id
                    AND current_run.is_current
                GROUP BY
                    source.id,
                    source.xid,
                    source.provider,
                    source.file_path,
                    current_run.status
                ORDER BY last_updated_at DESC NULLS LAST, source.id DESC
            """)
        )
        rows = result.fetchall()
    await engine.dispose()
    return [
        {
            "source_xid": str(row[0]),
            "provider": row[1],
            "file_path": row[2],
            "run_count": row[3],
            "last_updated_at": row[4],
            "current_status": row[5] or "not started",
        }
        for row in rows
    ]


async def _mark_interrupted_runs(connection_string: str) -> None:
    engine = create_async_engine(
        connection_string.replace("postgresql://", "postgresql+asyncpg://", 1)
    )
    async with engine.begin() as connection:
        await connection.execute(
            text("""
                UPDATE ingestion_runs
                SET status = 'failed',
                    error = 'Ingestion was interrupted before completion',
                    progress_stage = 'error',
                    progress_value = 0,
                    progress_message = 'Ingestion was interrupted before completion',
                    updated_at = CURRENT_TIMESTAMP
                WHERE status = 'running'
            """)
        )
    await engine.dispose()


def _mark_interrupted_ingestion_runs() -> None:
    asyncio.run(
        _mark_interrupted_runs(
            database_sql_connection_string(Dialect.POSTGRES)
        )
    )


def _ingestion_sources() -> list[dict[str, Any]]:
    return asyncio.run(
        _get_ingestion_sources(
            database_sql_connection_string(Dialect.POSTGRES)
        )
    )


async def _get_ingestion_runs(
    connection_string: str,
    source_xid: str,
) -> list[dict[str, Any]]:
    engine = create_async_engine(
        connection_string.replace("postgresql://", "postgresql+asyncpg://", 1)
    )
    async with engine.begin() as connection:
        result = await connection.execute(
            text("""
                SELECT
                    run.xid,
                    run.status,
                    run.error,
                    run.last_processed_record_id,
                    run.is_current,
                    run.created_at,
                    run.updated_at
                FROM ingestion_runs AS run
                INNER JOIN ingestion_sources AS source
                    ON source.id = run.ingestion_source_id
                WHERE source.xid = :source_xid
                ORDER BY run.created_at DESC, run.id DESC
            """),
            {"source_xid": source_xid},
        )
        rows = result.fetchall()
    await engine.dispose()
    return [
        {
            "run_xid": str(row[0]),
            "status": row[1],
            "error": row[2] or "",
            "last_processed_record_id": row[3] or "",
            "is_current": row[4],
            "created_at": row[5],
            "updated_at": row[6],
        }
        for row in rows
    ]


def _ingestion_runs(source_xid: str) -> list[dict[str, Any]]:
    return asyncio.run(
        _get_ingestion_runs(
            database_sql_connection_string(Dialect.POSTGRES),
            source_xid,
        )
    )


def _progress_value(data: dict[str, Any]) -> tuple[float, str]:
    stages = {
        "extract": (0.1, "Extracting resumes"),
        "transform_clean": (0.3, "Cleaning records"),
        "transform_enrich": (0.5, "Enriching records"),
        "transform_chunk": (0.7, "Creating knowledge chunks"),
        "load": (0.9, "Loading knowledge base"),
        "error": (0.0, "Workflow failed"),
    }
    return stages.get(data.get("stage"), (0.0, "Starting workflow"))


async def _ingest(
    provider: str,
    file_path: str,
    max_records_to_extract: int,
    parsing_mode: bool,
    progress: Any,
    event_log: Any,
) -> None:
    registry = DomainRegistry()
    service = registry.get("service.resume.langgraph")
    if not isinstance(service, LanggraphResumeService):
        raise RuntimeError("LanggraphResumeService is not registered in the DomainRegistry")

    log_entries: list[str] = []
    events = service.ingest_resume_records_to_knowledge_base(
        provider=provider,
        file_path=file_path,
        max_records_to_extract=max_records_to_extract,
        parsing_mode=parsing_mode,
    )
    async for event in events:
        data = event.get("data", {})
        progress_value, stage = _progress_value(data)
        if event.get("event") == "complete":
            progress_value = 1.0
        progress.progress(progress_value, text=stage)
        log_entries.append(
            " | ".join(
                [
                    stage,
                    f"Extracted: {data.get('extracted_count', 0)}",
                    f"Cleaned: {data.get('cleaned_count', 0)}",
                    f"Enriched: {data.get('enriched_count', 0)}",
                    f"Chunks: {data.get('chunked_count', 0)}",
                ]
            )
        )
        event_log.code("\n".join(log_entries))
        if data.get("error"):
            error = data["error"]
            raise RuntimeError(
                f"{error.get('node', 'Workflow')}: "
                f"{error.get('message', 'Unknown workflow error')}"
            )


def _store_uploaded_file(uploaded_file: Any) -> Path:
    file_bytes = uploaded_file.getvalue()
    file_hash = hashlib.sha256(file_bytes).hexdigest()
    UPLOAD_DIRECTORY.mkdir(parents=True, exist_ok=True)
    destination = UPLOAD_DIRECTORY / f"{file_hash}.csv"
    if not destination.exists():
        destination.write_bytes(file_bytes)
    return destination


st.set_page_config(
    page_title="Resume Ingestion",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="collapsed",
)
st.title("Resume ingestion")
st.caption("Process resume records into the knowledge base with resumable runs.")

missing_secrets = [key for key in REQUIRED_SECRETS if key not in st.secrets]
if missing_secrets:
    st.error(
        f"Missing secrets: {', '.join(missing_secrets)}. "
        "Add them to .streamlit/secrets.toml."
    )
    st.stop()

try:
    initialize_database()
    _mark_interrupted_ingestion_runs()
except Exception as error:
    st.error(f"Database initialization failed: {error}")
    st.stop()

st.subheader("Ingestion sources")
source_records = _ingestion_sources()
if source_records:
    header_columns = st.columns((2, 1, 4, 1, 2, 2, 1))
    for column, label in zip(
        header_columns,
        (
            "Source XID",
            "Provider",
            "File path",
            "Runs",
            "Last updated",
            "Current status",
            "Action",
        ),
    ):
        column.markdown(f"**{label}**")

    for source in source_records:
        row_columns = st.columns((2, 1, 4, 1, 2, 2, 1))
        row_columns[0].write(source["source_xid"])
        row_columns[1].write(source["provider"])
        row_columns[2].write(source["file_path"])
        row_columns[3].write(source["run_count"])
        row_columns[4].write(source["last_updated_at"])
        row_columns[5].write(source["current_status"])
        if row_columns[6].button(
            "Details",
            key=f"source-details-{source['source_xid']}",
            width="stretch",
        ):
            st.session_state["selected_source_xid"] = source["source_xid"]
            st.rerun()
else:
    st.info("No ingestion sources have been created yet.")

selected_source_xid = st.session_state.get("selected_source_xid")
selected_source = next(
    (
        source
        for source in source_records
        if source["source_xid"] == selected_source_xid
    ),
    None,
)
if selected_source is not None:
    st.divider()
    detail_header, detail_action = st.columns((5, 1))
    detail_header.subheader("Ingestion Run history")
    if detail_action.button("Close", width="stretch"):
        st.session_state.pop("selected_source_xid", None)
        st.rerun()
    st.caption(
        f"{selected_source['provider']} · {selected_source['file_path']}"
    )
    run_records = _ingestion_runs(selected_source["source_xid"])
    if not run_records:
        st.info("No ingestion runs exist for this source.")
    else:
        run_header = st.columns((2, 1, 3, 2, 1, 2, 2))
        for column, label in zip(
            run_header,
            (
                "Run XID",
                "Status",
                "Error",
                "Last processed ID",
                "Current",
                "Created",
                "Updated",
            ),
        ):
            column.markdown(f"**{label}**")
        for run in run_records:
            run_columns = st.columns((2, 1, 3, 2, 1, 2, 2))
            run_columns[0].write(run["run_xid"])
            run_columns[1].write(run["status"])
            run_columns[2].write(run["error"])
            run_columns[3].write(run["last_processed_record_id"])
            run_columns[4].write("Yes" if run["is_current"] else "No")
            run_columns[5].write(run["created_at"])
            run_columns[6].write(run["updated_at"])

provider = st.selectbox("Provider", ["File", "URL"], index=0).strip().lower()
if provider == "file":
    uploaded_file = st.file_uploader("Upload resume CSV", type=["csv"])
    if uploaded_file is None:
        st.info("Upload a CSV file to begin.")
        st.stop()
    selected_file_path = _store_uploaded_file(uploaded_file)
    file_path = str(selected_file_path.resolve())
else:
    file_path = st.text_input(
        "CSV URL",
        placeholder="https://example.com/resumes.csv",
    ).strip()
    if not file_path:
        st.info("Enter a CSV URL to begin.")
        st.stop()

current_status = _status(provider, file_path)
is_retry = current_status and current_status["status"] == "failed"
if not is_retry:
    max_records_to_extract = st.number_input(
        "Records to ingest in this run",
        min_value=1,
        value=10,
        step=1,
        help="Sets the maximum number of new records processed by one ingestion run.",
    )
    parsing_mode = st.checkbox(
        "Enable LLM parsing",
        value=False,
        help="Parse resume text into structured fields using an LLM. Disable for faster ingestion without enrichment.",
    )
else:
    max_records_to_extract = 10
    parsing_mode = False

if current_status:
    status_value = current_status["status"]
    status_label = {
        "running": "Running",
        "succeeded": "Succeeded",
        "failed": "Failed - retry available",
    }.get(status_value, status_value.title())
    columns = st.columns(4)
    columns[0].metric("Status", status_label)
    columns[1].metric("Source XID", str(current_status["source_xid"]))
    columns[2].metric("Last processed ID", current_status["last_processed_record_id"] or "-")
    columns[3].metric("Run XID", str(current_status["run_xid"])[:12] + "...")
    if current_status["progress_message"]:
        st.progress(
            float(current_status["progress_value"] or 0.0),
            text=current_status["progress_message"],
        )
    if current_status["error"]:
        st.warning(current_status["error"])
else:
    st.info("This source has not been ingested yet. A run will be created when ingestion starts.")

run_label = "Retry failed ingestion" if current_status and current_status["status"] == "failed" else "Run ingestion"
run_column = st.columns(1)[0]
run_clicked = run_column.button(
    run_label,
    type="primary",
    width="stretch",
    disabled=bool(
        current_status
        and current_status["status"] == "running"
    ),
)

if run_clicked:
    progress = st.progress(0.0, text="Starting workflow")
    event_log = st.empty()
    with st.status("Ingesting resumes...", expanded=True) as status:
        try:
            asyncio.run(
                _ingest(
                    provider,
                    file_path,
                    int(max_records_to_extract),
                    parsing_mode,
                    progress,
                    event_log,
                )
            )
            progress.progress(1.0, text="Ingestion complete")
            status.update(label="Ingestion complete", state="complete", expanded=False)
            st.rerun()
        except Exception as error:
            status.update(label="Ingestion failed", state="error", expanded=True)
            st.exception(error)
