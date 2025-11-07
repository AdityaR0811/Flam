"""SQLAlchemy models for queuectl database."""

from datetime import datetime
from typing import Optional

from sqlalchemy import Index, Integer, String, Text, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all models."""

    pass


class Job(Base):
    """Job model representing a background task."""

    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    command: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(
        String(50), nullable=False, default="pending", index=True
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    backoff_base: Mapped[float] = mapped_column(nullable=False, default=2.0)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0, index=True)
    
    # Scheduling and timeout
    run_at: Mapped[datetime] = mapped_column(nullable=False, index=True)
    timeout_s: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    updated_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    
    # Locking
    locked_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    locked_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    
    # Execution results
    last_exit_code: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    stdout: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stderr: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    __table_args__ = (
        Index("idx_jobs_polling", "state", "run_at", "priority"),
        Index("idx_jobs_locked_by", "locked_by"),
    )


class Config(Base):
    """Configuration key-value store."""

    __tablename__ = "config"

    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)


class Worker(Base):
    """Worker process registry for crash recovery."""

    __tablename__ = "workers"

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    started_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    last_heartbeat: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="active")
