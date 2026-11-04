from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from backend.resume_ingestion.internal.domain.repository.resume.database.vector.langchain.init import (
        LangchainVectorResumeRepository,
    )

from typing import Any
from uuid import NAMESPACE_URL, uuid5

from langchain_core.documents import Document

class StoreRecordsMixin:
    async def store_records(self: LangchainVectorResumeRepository, records: list[dict[str, Any]]) -> None:
        documents = []
        ids = []
        for record in records:
            if record.get("ID") is None:
                continue
            source_id = str(record["ID"])
            metadata = {
                **(record.get("metadata", {}) or {}),
                "source_id": source_id,
            }
            point_id = str(uuid5(NAMESPACE_URL, f"hr-assist:resume:{source_id}"))
            documents.append(
                Document(
                    id=point_id,
                    page_content=str(record.get("chunk", "")),
                    metadata=metadata,
                )
            )
            ids.append(point_id)
        if documents:
            self._vector_store.add_documents(
                documents,
                ids=ids,
            )