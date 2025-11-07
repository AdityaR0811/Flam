"""Tests for job enqueueing functionality."""

import json
import tempfile
from pathlib import Path

import pytest

from queuectl.db import get_session, init_db
from queuectl.repo import JobRepository
from queuectl.utils.time import ensure_utc, utcnow


@pytest.fixture
def temp_db():
    """Create temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    init_db(db_path)
    yield db_path
    
    # Cleanup
    Path(db_path).unlink(missing_ok=True)


def test_enqueue_simple_job(temp_db):
    """Test enqueueing a simple job."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        job = repo.create_job(command="echo hello", job_id="test-1")
        
        assert job.id == "test-1"
        assert job.command == "echo hello"
        assert job.state == "pending"
        assert job.attempts == 0
        assert job.priority == 0


def test_enqueue_with_priority(temp_db):
    """Test enqueueing job with priority."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        job = repo.create_job(command="echo test", priority=10)
        
        assert job.priority == 10


def test_enqueue_with_schedule(temp_db):
    """Test enqueueing job with future run_at."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        future = utcnow().replace(microsecond=0)
        job = repo.create_job(command="echo scheduled", run_at=future)
        
        # Compare timestamps (job.run_at might be naive after DB round-trip)
        time_diff = abs((job.run_at.replace(tzinfo=None) - future.replace(tzinfo=None)).total_seconds())
        assert time_diff < 1


def test_enqueue_duplicate_id_fails(temp_db):
    """Test that duplicate job IDs are rejected."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        repo.create_job(command="echo first", job_id="dup-1")
        
        with pytest.raises(ValueError, match="already exists"):
            repo.create_job(command="echo second", job_id="dup-1")


def test_enqueue_auto_generates_id(temp_db):
    """Test that job ID is auto-generated when not provided."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        job = repo.create_job(command="echo auto")
        
        assert job.id is not None
        assert len(job.id) > 0


def test_enqueue_config_snapshot(temp_db):
    """Test that job captures config snapshot on creation."""
    with next(get_session(temp_db)) as session:
        from queuectl.config import ConfigManager
        
        config = ConfigManager(session)
        config.set("max_retries", 5)
        config.set("backoff_base", 3.0)
        
        repo = JobRepository(session)
        job = repo.create_job(command="echo test")
        
        assert job.max_retries == 5
        assert job.backoff_base == 3.0


def test_enqueue_custom_timeout(temp_db):
    """Test enqueueing job with custom timeout."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        job = repo.create_job(command="sleep 10", timeout_s=5)
        
        assert job.timeout_s == 5


def test_enqueue_validates_run_at_format(temp_db):
    """Test that run_at accepts ISO format strings."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        # ISO format string
        job = repo.create_job(command="echo test", run_at="2025-01-01T00:00:00Z")
        
        assert job.run_at.year == 2025
        assert job.run_at.month == 1


def test_list_jobs_by_state(temp_db):
    """Test listing jobs filtered by state."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        repo.create_job(command="echo 1", job_id="job-1")
        repo.create_job(command="echo 2", job_id="job-2")
        
        # Mark one as completed
        job2 = repo.get_job("job-2")
        repo.mark_success("job-2", 0, "output", "", 100)
        
        pending = repo.list_jobs(state="pending")
        completed = repo.list_jobs(state="completed")
        
        assert len(pending) == 1
        assert pending[0].id == "job-1"
        assert len(completed) == 1
        assert completed[0].id == "job-2"


def test_list_jobs_with_limit(temp_db):
    """Test listing jobs with limit."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        for i in range(10):
            repo.create_job(command=f"echo {i}")
        
        jobs = repo.list_jobs(limit=5)
        
        assert len(jobs) == 5
