from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.resume_ingestion.internal.domain.model.database.sql.sqlalchemy.base import Base


class IngestionSource(Base):
    __tablename__ = "ingestion_sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    xid: Mapped[str] = mapped_column(String(36), unique=True, nullable=False)
    provider: Mapped[str] = mapped_column(String, nullable=False)
    file_path: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint("provider", "file_path", name="uq_ingestion_sources_provider_file_path"),
    )

    runs: Mapped[list["IngestionRun"]] = relationship(
        "IngestionRun",
        back_populates="source",
        cascade="all, delete-orphan",
    )
