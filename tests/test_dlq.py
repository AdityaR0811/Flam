"""Tests for dead letter queue functionality."""

import tempfile
from pathlib import Path

import pytest

from queuectl.db import get_session, init_db
from queuectl.repo import JobRepository


@pytest.fixture
def temp_db():
    """Create temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    init_db(db_path)
    yield db_path
    
    Path(db_path).unlink(missing_ok=True)


def test_failed_job_moves_to_dlq(temp_db):
    """Test that job moves to DLQ after max retries."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        # Create job with 2 max retries
        job = repo.create_job(command="false", job_id="dlq-1", max_retries=2)
        
        # Fail it twice
        for _ in range(2):
            repo.mark_failure(job.id, 1, "", "error", 100)
        
        # Check it's in DLQ
        dlq_jobs = repo.list_dlq_jobs()
        
        assert len(dlq_jobs) == 1
        assert dlq_jobs[0].id == "dlq-1"
        assert dlq_jobs[0].state == "dead"
        assert dlq_jobs[0].attempts == 2


def test_dlq_list_shows_dead_jobs(temp_db):
    """Test that DLQ list shows only dead jobs."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        # Create various jobs
        repo.create_job(command="echo ok", job_id="ok-1")
        repo.create_job(command="false", job_id="fail-1", max_retries=1)
        
        # Mark fail-1 as dead
        repo.mark_failure("fail-1", 1, "", "error", 100)
        
        # Mark ok-1 as completed
        repo.mark_success("ok-1", 0, "output", "", 100)
        
        # DLQ should only show dead jobs
        dlq_jobs = repo.list_dlq_jobs()
        
        assert len(dlq_jobs) == 1
        assert dlq_jobs[0].id == "fail-1"


def test_dlq_retry_moves_job_to_pending(temp_db):
    """Test that DLQ retry moves job back to pending."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        # Create and move job to DLQ
        job = repo.create_job(command="false", job_id="retry-test", max_retries=1)
        repo.mark_failure(job.id, 1, "", "error", 100)
        
        # Verify it's dead
        job = repo.get_job("retry-test")
        assert job.state == "dead"
        
        # Retry from DLQ
        success = repo.retry_dlq_job("retry-test")
        
        assert success
        
        # Check it's back in pending
        job = repo.get_job("retry-test")
        assert job.state == "pending"
        assert job.attempts == 0


def test_dlq_retry_resets_attempts(temp_db):
    """Test that DLQ retry resets attempt counter."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        job = repo.create_job(command="false", job_id="reset-test", max_retries=2)
        
        # Fail twice
        repo.mark_failure(job.id, 1, "", "error", 100)
        repo.mark_failure(job.id, 1, "", "error", 100)
        
        # Retry
        repo.retry_dlq_job("reset-test")
        
        job = repo.get_job("reset-test")
        assert job.attempts == 0


def test_dlq_retry_updates_run_at(temp_db):
    """Test that DLQ retry schedules job to run now."""
    from queuectl.utils.time import utcnow
    
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        job = repo.create_job(command="false", job_id="runAt-test", max_retries=1)
        original_run_at = job.run_at
        
        # Move to DLQ
        repo.mark_failure(job.id, 1, "", "error", 100)
        
        # Retry
        repo.retry_dlq_job("runAt-test")
        
        job = repo.get_job("runAt-test")
        
        # run_at should be updated to now
        assert job.run_at >= original_run_at


def test_dlq_retry_nonexistent_job(temp_db):
    """Test that retrying nonexistent job returns False."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        success = repo.retry_dlq_job("nonexistent")
        
        assert success is False


def test_dlq_retry_non_dead_job(temp_db):
    """Test that retrying non-dead job returns False."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        job = repo.create_job(command="echo test", job_id="not-dead")
        
        # Try to retry pending job
        success = repo.retry_dlq_job("not-dead")
        
        assert success is False


def test_dlq_preserves_job_output(temp_db):
    """Test that DLQ preserves job stdout/stderr."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        job = repo.create_job(command="false", job_id="output-test", max_retries=1)
        
        # Fail with output
        repo.mark_failure(job.id, 1, "stdout output", "stderr error", 100)
        
        # Check DLQ job has output
        dlq_jobs = repo.list_dlq_jobs()
        
        assert len(dlq_jobs) == 1
        assert dlq_jobs[0].stdout == "stdout output"
        assert dlq_jobs[0].stderr == "stderr error"


def test_dlq_preserves_exit_code(temp_db):
    """Test that DLQ preserves exit code."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        job = repo.create_job(command="exit 42", job_id="exitcode-test", max_retries=1)
        
        # Fail with specific exit code
        repo.mark_failure(job.id, 42, "", "error", 100)
        
        # Check exit code in DLQ
        dlq_jobs = repo.list_dlq_jobs()
        
        assert len(dlq_jobs) == 1
        assert dlq_jobs[0].last_exit_code == 42


def test_multiple_jobs_in_dlq(temp_db):
    """Test handling multiple jobs in DLQ."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        # Create and fail multiple jobs
        for i in range(5):
            job = repo.create_job(command=f"false {i}", job_id=f"dlq-multi-{i}", max_retries=1)
            repo.mark_failure(job.id, 1, "", f"error {i}", 100)
        
        # Check DLQ
        dlq_jobs = repo.list_dlq_jobs()
        
        assert len(dlq_jobs) == 5


def test_dlq_limit(temp_db):
    """Test that DLQ list respects limit."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        # Create many dead jobs
        for i in range(10):
            job = repo.create_job(command=f"false {i}", job_id=f"limit-{i}", max_retries=1)
            repo.mark_failure(job.id, 1, "", "error", 100)
        
        # List with limit
        dlq_jobs = repo.list_dlq_jobs(limit=5)
        
        assert len(dlq_jobs) == 5
