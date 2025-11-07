"""Database initialization and session management."""

import os
from pathlib import Path
from typing import Generator

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from queuectl.models import Base


# Default database location
DEFAULT_DB_PATH = Path.home() / ".queuectl" / "queue.db"


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_conn, connection_record):
    """Enable foreign keys and WAL mode for better concurrency."""
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.close()


def get_db_url(db_path: str | Path | None = None) -> str:
    """Get database URL.
    
    Args:
        db_path: Optional custom database path.
        
    Returns:
        SQLite database URL.
    """
    if db_path is None:
        db_path = os.environ.get("QUEUECTL_DB_PATH", str(DEFAULT_DB_PATH))
    
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    return f"sqlite:///{path}"


def create_db_engine(db_path: str | Path | None = None):
    """Create database engine with proper configuration.
    
    Args:
        db_path: Optional custom database path.
        
    Returns:
        SQLAlchemy engine.
    """
    url = get_db_url(db_path)
    engine = create_engine(
        url,
        echo=False,
        pool_pre_ping=True,
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    return engine


def init_db(db_path: str | Path | None = None) -> Engine:
    """Initialize database, creating tables if needed.
    
    Args:
        db_path: Optional custom database path.
        
    Returns:
        Database engine.
    """
    engine = create_db_engine(db_path)
    Base.metadata.create_all(engine)
    
    # Seed default config
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        _seed_default_config(session)
    
    return engine


def _seed_default_config(session: Session) -> None:
    """Seed default configuration values if not present.
    
    Args:
        session: Database session.
    """
    from queuectl.models import Config
    
    defaults = {
        "max_retries": "3",
        "backoff_base": "2.0",
        "poll_interval_ms": "500",
        "lock_timeout_s": "300",
        "job_timeout_s": "0",  # 0 means no timeout
        "max_backoff_s": "3600",
    }
    
    for key, value in defaults.items():
        existing = session.get(Config, key)
        if existing is None:
            session.add(Config(key=key, value=value))
    
    session.commit()


def get_session(db_path: str | Path | None = None) -> Generator[Session, None, None]:
    """Get database session as context manager.
    
    Args:
        db_path: Optional custom database path.
        
    Yields:
        Database session.
    """
    engine = create_db_engine(db_path)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
