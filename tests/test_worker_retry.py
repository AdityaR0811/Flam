"""Tests for worker retry logic and backoff."""

import tempfile
import time
from pathlib import Path

import pytest

from queuectl.db import get_session, init_db
from queuectl.locking import calculate_backoff_delay
from queuectl.repo import JobRepository


@pytest.fixture
def temp_db():
    """Create temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    init_db(db_path)
    yield db_path
    
    Path(db_path).unlink(missing_ok=True)


def test_failed_job_retries(temp_db):
    """Test that failed jobs are retried."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        # Create job with max_retries=3
        job = repo.create_job(command="false", max_retries=3)
        original_run_at = job.run_at
        
        # Mark as failed
        repo.mark_failure(job.id, 1, "", "error", 100)
        
        # Refresh job
        updated_job = repo.get_job(job.id)
        
        assert updated_job.attempts == 1
        assert updated_job.state == "failed"
        # Run time should be scheduled later after backoff
        assert updated_job.run_at > original_run_at


def test_job_moves_to_dlq_after_max_retries(temp_db):
    """Test that job moves to DLQ after exhausting retries."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        job = repo.create_job(command="false", max_retries=2)
        
        # Fail twice
        for _ in range(2):
            repo.mark_failure(job.id, 1, "", "error", 100)
        
        # Check job is in DLQ
        updated_job = repo.get_job(job.id)
        
        assert updated_job.state == "dead"
        assert updated_job.attempts == 2


def test_backoff_delay_calculation():
    """Test exponential backoff calculation."""
    # Base 2, no max
    delay1 = calculate_backoff_delay(1, 2.0, 3600)
    delay2 = calculate_backoff_delay(2, 2.0, 3600)
    delay3 = calculate_backoff_delay(3, 2.0, 3600)
    
    # Should roughly double (with jitter)
    assert 2.0 <= delay1 <= 3.0  # 2^1 = 2, jitter up to 1
    assert 4.0 <= delay2 <= 5.0  # 2^2 = 4
    assert 8.0 <= delay3 <= 9.0  # 2^3 = 8


def test_backoff_respects_max(temp_db):
    """Test that backoff respects max_backoff_s."""
    delay = calculate_backoff_delay(10, 2.0, 100)
    
    # 2^10 = 1024, but max is 100
    assert delay <= 101.0  # 100 + max jitter of 1


def test_backoff_has_jitter():
    """Test that jitter is applied to backoff."""
    delays = [calculate_backoff_delay(1, 2.0, 3600) for _ in range(10)]
    
    # All delays should be different due to jitter
    assert len(set(delays)) > 1


def test_successful_job_does_not_retry(temp_db):
    """Test that successful jobs are not retried."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        job = repo.create_job(command="echo success")
        
        # Mark as success
        repo.mark_success(job.id, 0, "output", "", 100)
        
        updated_job = repo.get_job(job.id)
        
        assert updated_job.state == "completed"
        assert updated_job.attempts == 0
        assert updated_job.locked_by is None


def test_retry_resets_attempts(temp_db):
    """Test that DLQ retry resets attempts."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        job = repo.create_job(command="false", max_retries=1)
        
        # Exhaust retries
        repo.mark_failure(job.id, 1, "", "error", 100)
        
        # Verify in DLQ
        dlq_job = repo.get_job(job.id)
        assert dlq_job.state == "dead"
        assert dlq_job.attempts == 1
        
        # Retry from DLQ
        repo.retry_dlq_job(job.id)
        
        retried_job = repo.get_job(job.id)
        assert retried_job.state == "pending"
        assert retried_job.attempts == 0


def test_output_truncation(temp_db):
    """Test that stdout/stderr are truncated to 8KB."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        job = repo.create_job(command="echo test")
        
        # Create output larger than 8KB
        large_output = "x" * 10000
        
        repo.mark_success(job.id, 0, large_output, large_output, 100)
        
        updated_job = repo.get_job(job.id)
        
        assert len(updated_job.stdout) == 8192
        assert len(updated_job.stderr) == 8192


def test_exit_code_stored(temp_db):
    """Test that exit code is stored correctly."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        job = repo.create_job(command="exit 42")
        
        repo.mark_failure(job.id, 42, "", "error", 100)
        
        updated_job = repo.get_job(job.id)
        
        assert updated_job.last_exit_code == 42


def test_duration_recorded(temp_db):
    """Test that execution duration is recorded."""
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        job = repo.create_job(command="sleep 0.1")
        
        repo.mark_success(job.id, 0, "", "", 150)
        
        updated_job = repo.get_job(job.id)
        
        assert updated_job.duration_ms == 150


# Property-based test using Hypothesis
from hypothesis import given, strategies as st


@given(
    attempts=st.integers(min_value=0, max_value=10),
    base=st.floats(min_value=1.1, max_value=5.0),
    max_backoff=st.integers(min_value=10, max_value=7200),
)
def test_backoff_properties(attempts, base, max_backoff):
    """Property-based test for backoff calculation."""
    delay = calculate_backoff_delay(attempts, base, max_backoff)
    
    # Delay should always be positive
    assert delay > 0
    
    # Delay should not exceed max + jitter
    assert delay <= max_backoff + (0.5 * base) + 1
    
    # Delay should include base exponential component
    if base ** attempts <= max_backoff:
        assert delay >= base ** attempts
