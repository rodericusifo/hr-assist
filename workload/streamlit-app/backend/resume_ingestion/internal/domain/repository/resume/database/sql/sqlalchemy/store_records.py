from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from backend.resume_ingestion.internal.domain.model.database.sql.sqlalchemy.resumes import (
    Resume,
)

if TYPE_CHECKING:
    from backend.resume_ingestion.internal.domain.repository.resume.database.sql.sqlalchemy.init import (
        SQLAlchemyDatabaseResumeRepository,
    )


class StoreRecordsMixin:
    def store_records(
        self: SQLAlchemyDatabaseResumeRepository,
        records: list[dict[str, Any]],
    ) -> None:
        values = [self._record_values(record) for record in records]
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

        with sessionmaker(self._engine, class_=Session)() as session:
            session.execute(statement)
            session.commit()

    @staticmethod
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
