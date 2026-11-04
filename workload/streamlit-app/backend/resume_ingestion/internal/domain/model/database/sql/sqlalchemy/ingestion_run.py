from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.resume_ingestion.internal.domain.model.database.sql.sqlalchemy.base import Base


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    xid: Mapped[str] = mapped_column(String(36), unique=True, nullable=False)
    ingestion_source_id: Mapped[int] = mapped_column(
        ForeignKey("ingestion_sources.id", ondelete="CASCADE"),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(String, nullable=False, default="running")
    error: Mapped[str | None] = mapped_column(Text)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_processed_record_id: Mapped[str | None] = mapped_column(Text)
    progress_stage: Mapped[str | None] = mapped_column(String)
    progress_value: Mapped[float | None] = mapped_column(Float)
    progress_message: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    source: Mapped["IngestionSource"] = relationship(
        "IngestionSource",
        back_populates="runs",
    )
