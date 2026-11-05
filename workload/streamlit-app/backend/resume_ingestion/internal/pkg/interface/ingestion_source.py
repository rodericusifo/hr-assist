from typing_extensions import Protocol, runtime_checkable

@runtime_checkable
class ISQLIngestionSourceRepository(Protocol):
    def get_or_create(
        self,
        provider: str,
        file_path: str,
    ) -> int:
        """Get or create an ingestion source."""

