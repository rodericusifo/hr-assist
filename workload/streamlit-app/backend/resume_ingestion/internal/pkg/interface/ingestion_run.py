from typing_extensions import Protocol

class IIngestionRunRepository(Protocol):
    def get_or_create(
            self,
            source_id: int,
        ) -> tuple[str, str | None, bool]:
            """Get or create the current ingestion run.
            
            Returns (xid, last_processed_record_id, is_retry).
            """
    def set_status(
        self,
        xid: str,
        status: str,
        error: str | None,
    ) -> None:
        """Update an ingestion run status."""

    def set_progress(
        self,
        xid: str,
        projection: dict,
    ) -> None:
        """Update an ingestion run progress."""
