import tempfile
import unittest
from uuid import UUID
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
from backend.resume_ingestion.internal.domain.repository.resume.database.sql.sqlalchemy.store_records import (
    StoreRecordsMixin,
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

            for provider in ("file", "url", "File", "URL"):
                state = IngestResumeRecordsToKnowledgeBaseWorkflowState(
                    provider=provider,
                    file_path=str(path),
                )
                result = ExtractMixin().extract(state, runtime)
                self.assertEqual(
                    [record["ID"] for record in result["extracted_records"]],
                    [1, 2],
                )

    def test_extract_starts_after_starting_record_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "resumes.csv"
            path.write_text(
                "ID,Resume_str,Category\n1,first,tech\n2,second,tech\n3,third,tech\n"
            )
            state = IngestResumeRecordsToKnowledgeBaseWorkflowState(
                provider="file",
                file_path=str(path),
                starting_record_id="1",
            )

            result = ExtractMixin().extract(
                state,
                SimpleNamespace(context=SimpleNamespace(max_records_to_extract=10)),
            )

            self.assertEqual(
                [record["ID"] for record in result["extracted_records"]],
                [2, 3],
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

    def test_ui_projection_contains_progress_summary_without_records(self) -> None:
        projection = IngestResumeRecordsToKnowledgeBaseMixin._ui_projection(
            {
                "extracted_records": [{"ID": "1", "Resume_str": "private"}],
                "transform_cleaned_records": [{"ID": "1"}],
                "transform_enriched_records": [{"ID": "1", "metadata": {}}],
                "transform_chunked_records": [
                    {"ID": "1_0000", "chunk": "private"}
                ],
                "last_processed_record_id": "1",
            }
        )

        self.assertEqual(projection["stage"], "load")
        self.assertEqual(projection["status"], "running")
        self.assertEqual(projection["extracted_count"], 1)
        self.assertEqual(projection["chunked_count"], 1)
        self.assertNotIn("Resume_str", projection)
        self.assertNotIn("chunk", projection)

    def test_last_processed_id_comes_from_last_extracted_record(self) -> None:
        self.assertEqual(
            IngestResumeRecordsToKnowledgeBaseMixin._last_processed_record_id(
                {"extracted_records": [{"ID": 3}, {"ID": 8}]}
            ),
            "8",
        )

    def test_vector_repository_stores_chunk_documents(self) -> None:
        vector_store = Mock()
        repository = QdrantLangchainVectorDatabaseResumeRepository(
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
        self.assertIsNotNone(UUID(documents[0].id))
        self.assertEqual(documents[0].page_content, "Resume text")
        self.assertEqual(documents[0].metadata["source_id"], "resume_0001")

    def test_load_node_stores_chunked_records(self) -> None:
        repository = Mock()
        workflow = SimpleNamespace(
            _vector_database_resume_repository=repository,
        )
        state = IngestResumeRecordsToKnowledgeBaseWorkflowState(
            extracted_records=[{"ID": "resume_0001"}],
            transform_chunked_records=[{"ID": "resume_0001", "chunk": "Resume text"}]
        )

        result = LoadMixin.load_to_vector_db(workflow, state)

        repository.embed_and_store_records.assert_called_once_with(
            state.transform_chunked_records
        )
        self.assertEqual(
            result,
            {"last_processed_record_id": "resume_0001"},
        )

    def test_load_to_sql_db_stores_enriched_records(self) -> None:
        repository = Mock()
        workflow = SimpleNamespace(
            _sql_database_resume_repository=repository,
        )
        state = IngestResumeRecordsToKnowledgeBaseWorkflowState(
            transform_enriched_records=[
                {
                    "ID": "resume_0001",
                    "Resume_str": "Resume text",
                    "Category": "tech",
                    "metadata": {"name": "Ada"},
                }
            ]
        )

        result = LoadMixin.load_to_sql_db(workflow, state)

        repository.store_records.assert_called_once_with(
            state.transform_enriched_records
        )
        self.assertEqual(result, {})

    def test_sql_repository_maps_enriched_record_fields(self) -> None:
        values = StoreRecordsMixin._record_values(
            {
                "ID": "resume_0001",
                "Resume_str": "Resume text",
                "Category": "tech",
                "metadata": {
                    "name": "Ada",
                    "contact": {"email": "ada@example.com"},
                    "experience": [],
                    "education": [],
                    "skills": ["python"],
                },
            }
        )

        self.assertEqual(values["id"], "resume_0001")
        self.assertIsNotNone(UUID(values["xid"]))
        self.assertEqual(values["resume_text"], "Resume text")
        self.assertEqual(values["name"], "Ada")
        self.assertEqual(values["skills"], ["python"])

    def test_sql_repository_rejects_missing_resume_id(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires an ID"):
            StoreRecordsMixin._record_values({"metadata": {}})


if __name__ == "__main__":
    unittest.main()
