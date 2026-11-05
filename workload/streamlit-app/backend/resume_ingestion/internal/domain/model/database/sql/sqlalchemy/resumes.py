from __future__ import annotations

from typing import Any

from sqlalchemy import Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class Resume(Base):
    __tablename__ = "resumes"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    xid: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    resume_text: Mapped[str] = mapped_column(Text, nullable=False)
    resume_html: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str | None] = mapped_column(Text)
    contact: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    summary: Mapped[str | None] = mapped_column(Text)
    experience: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    education: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    skills: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
