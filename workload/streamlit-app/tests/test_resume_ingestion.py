import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from langchain_core.documents import Document

from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.ingest_resume_records_to_knowledge_base import (
    IngestResumeRecordsToKnowledgeBaseMixin,
)
from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.extract import (
    ExtractMixin,
)
from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.load import (
    LoadMixin,
)
from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.schema import (
    IngestResumeRecordsToKnowledgeBaseWorkflowState,
)
from backend.resume_ingestion.internal.domain.repository.resume.database.vector.langchain.qdrant.init import (
    QdrantLangchainVectorDatabaseResumeRepository,
)


class ResumeIngestionTests(unittest.TestCase):
    def test_extract_supports_file_and_url_providers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "resumes.csv"
            path.write_text(
                "ID,Resume_str,Category\n2,second,tech\n1,first,tech\n"
            )
            runtime = SimpleNamespace(
                context=SimpleNamespace(max_records_to_extract=10)
            )

            for provider in ("file", "url"):
                state = IngestResumeRecordsToKnowledgeBaseWorkflowState(
                    provider=provider,
                    file_path=str(path),
                )
                result = ExtractMixin().extract(state, runtime)
                self.assertEqual(
                    [record["ID"] for record in result["extracted_records"]],
                    [1, 2],
                )

    def test_extract_rejects_unknown_provider(self) -> None:
        state = IngestResumeRecordsToKnowledgeBaseWorkflowState(
            provider="unknown",
            file_path="resumes.csv",
        )
        with self.assertRaisesRegex(ValueError, "Unsupported provider"):
            ExtractMixin().extract(state, SimpleNamespace(context=SimpleNamespace()))

    def test_error_snapshot_is_detected(self) -> None:
        self.assertTrue(
            IngestResumeRecordsToKnowledgeBaseMixin._snapshot_has_error(
                {"error": {"message": "failed"}}
            )
        )
        self.assertFalse(
            IngestResumeRecordsToKnowledgeBaseMixin._snapshot_has_error(
                {"error": None}
            )
        )

    def test_last_processed_id_comes_from_last_extracted_record(self) -> None:
        self.assertEqual(
            IngestResumeRecordsToKnowledgeBaseMixin._last_processed_record_id(
                {"extracted_records": [{"ID": 3}, {"ID": 8}]}
            ),
            "8",
        )

    def test_vector_repository_stores_chunk_documents(self) -> None:
        vector_store = Mock()
        llm = Mock()
        repository = QdrantLangchainVectorDatabaseResumeRepository(
            llm=llm,
            vector_store=vector_store,
        )

        repository.embed_and_store_records(
            [
                {
                    "ID": "resume_0001",
                    "chunk": "Resume text",
                    "metadata": {"category": "tech"},
                }
            ]
        )

        vector_store.add_documents.assert_called_once()
        documents = vector_store.add_documents.call_args.args[0]
        self.assertIsInstance(documents[0], Document)
        self.assertEqual(documents[0].id, "resume_0001")
        self.assertEqual(documents[0].page_content, "Resume text")

    def test_load_node_stores_chunked_records(self) -> None:
        repository = Mock()
        workflow = SimpleNamespace(
            _vector_database_resume_repository=repository,
        )
        state = IngestResumeRecordsToKnowledgeBaseWorkflowState(
            extracted_records=[{"ID": "resume_0001"}],
            transform_chunked_records=[{"ID": "resume_0001", "chunk": "Resume text"}]
        )

        result = LoadMixin.load(workflow, state)

        repository.embed_and_store_records.assert_called_once_with(
            state.transform_chunked_records
        )
        self.assertIs(result, state)
        self.assertEqual(result.last_processed_record_id, "resume_0001")


if __name__ == "__main__":
    unittest.main()
