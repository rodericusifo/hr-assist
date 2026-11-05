from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    from .init import (
        SQLAlchemyResumeRepository,
    )

from uuid import uuid4

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from backend.resume_ingestion.internal.domain.model.database.sql.sqlalchemy.resumes import (
    Resume,
)

async def store_records(
    self: SQLAlchemyResumeRepository,
    records: list[dict[str, Any]],
) -> None:
    values = [_record_values(record) for record in records]
    if not values:
        return

    statement = insert(Resume).values(values)
    update_values = {
        column.name: getattr(statement.excluded, column.name)
        for column in Resume.__table__.columns
        if column.name not in {"id", "xid"}
    }
    statement = statement.on_conflict_do_update(
        index_elements=[Resume.id],
        set_=update_values,
    )

    async with AsyncSession(self._engine) as session:
        await session.execute(statement)
        await session.commit()

def _record_values(record: dict[str, Any]) -> dict[str, Any]:
    resume_id = record.get("ID")
    if resume_id is None or str(resume_id).strip() == "":
        raise ValueError("Enriched resume record requires an ID")

    metadata = record.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError(
            f"Enriched resume record {resume_id} requires metadata"
        )

    return {
        "id": str(resume_id),
        "xid": str(uuid4()),
        "resume_text": str(record.get("Resume_str", "")),
        "resume_html": str(record.get("Resume_html", "")),
        "category": str(record.get("Category", "")),
        "name": metadata.get("name"),
        "contact": metadata.get("contact", {}),
        "summary": metadata.get("summary"),
        "experience": metadata.get("experience", []),
        "education": metadata.get("education", []),
        "skills": metadata.get("skills", []),
    }
