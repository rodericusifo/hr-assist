from typing_extensions import Protocol, runtime_checkable

@runtime_checkable
class IResumeService(Protocol):
    def ingest_resume_records_to_knowledge_base(self, provider: str, file_path: str) -> dict:
        """Ingest resume records into the knowledge base."""

@runtime_checkable
class ISQLResumeRepository(Protocol):
    def store_records(self, records: list[dict]) -> None:
        """Store records in the SQL database."""

@runtime_checkable
class IVectorResumeRepository(Protocol):
    def embed_and_store_records(self, records: list[dict]) -> None:
        """Embed and store records in the vector database."""