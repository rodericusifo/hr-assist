from __future__ import annotations
from typing import TYPE_CHECKING
from typing import Any
if TYPE_CHECKING:
    from .init import IngestResumeRecordsToKnowledgeBaseWorkflow

from .schema import (
    IngestResumeRecordsToKnowledgeBaseWorkflowState,
)

async def load_to_sql_db(
    self: IngestResumeRecordsToKnowledgeBaseWorkflow,
    state: IngestResumeRecordsToKnowledgeBaseWorkflowState,
) -> dict[str, Any]:
    if self._sql_resume_repository is None:
        raise RuntimeError(
            "A resume SQL database repository is required to load records"
        )
    if state.transform_enriched_records is None:
        raise ValueError("Cannot load before enriching resume records")

    await self._sql_resume_repository.store_records(
        state.transform_enriched_records
    )
    return {}
