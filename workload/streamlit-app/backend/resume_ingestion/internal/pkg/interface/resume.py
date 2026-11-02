from typing_extensions import Protocol

class IResumeService(Protocol):
    def ingest_resume_records_to_knowledge_base(self, provider: str, file_path: str) -> dict:
        """Ingest resume records into the knowledge base."""