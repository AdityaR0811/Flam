"""Tests for data persistence across restarts."""

import tempfile
from pathlib import Path

import pytest

from queuectl.db import create_db_engine, get_session, init_db
from queuectl.repo import JobRepository


@pytest.fixture
def temp_db():
    """Create temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    init_db(db_path)
    yield db_path
    
    Path(db_path).unlink(missing_ok=True)


def test_jobs_persist_across_sessions(temp_db):
    """Test that jobs survive database reconnection."""
    # Create jobs in first session
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        repo.create_job(command="echo test1", job_id="persist-1")
        repo.create_job(command="echo test2", job_id="persist-2")
    
    # Retrieve jobs in new session
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        jobs = repo.list_jobs()
        
        assert len(jobs) == 2
        assert any(j.id == "persist-1" for j in jobs)
        assert any(j.id == "persist-2" for j in jobs)


def test_job_state_persists(temp_db):
    """Test that job state changes persist."""
    # Create and mark job as completed
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        job = repo.create_job(command="echo test", job_id="state-test")
        repo.mark_success(job.id, 0, "output", "", 100)
    
    # Check state in new session
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        job = repo.get_job("state-test")
        
        assert job.state == "completed"
        assert job.stdout == "output"
        assert job.duration_ms == 100


def test_config_persists(temp_db):
    """Test that configuration persists."""
    from queuectl.config import ConfigManager
    
    # Set config in first session
    with next(get_session(temp_db)) as session:
        config = ConfigManager(session)
        config.set("max_retries", 5)
        config.set("backoff_base", 3.0)
    
    # Retrieve config in new session
    with next(get_session(temp_db)) as session:
        config = ConfigManager(session)
        
        assert config.get_int("max_retries") == 5
        assert config.get_float("backoff_base") == 3.0


def test_worker_registry_persists(temp_db):
    """Test that worker registry persists."""
    from queuectl.repo import WorkerRepository
    
    # Register worker
    with next(get_session(temp_db)) as session:
        worker_repo = WorkerRepository(session)
        worker_repo.register_worker("persist-worker-1")
    
    # Check in new session
    with next(get_session(temp_db)) as session:
        worker_repo = WorkerRepository(session)
        active = worker_repo.get_active_workers(stale_threshold_s=60)
        
        assert len(active) == 1
        assert active[0].id == "persist-worker-1"


def test_job_attempts_persist(temp_db):
    """Test that job retry attempts persist."""
    # Create job and mark as failed
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        job = repo.create_job(command="false", job_id="attempts-test", max_retries=3)
        repo.mark_failure(job.id, 1, "", "error", 100)
    
    # Check attempts in new session
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        job = repo.get_job("attempts-test")
        
        assert job.attempts == 1
        assert job.state == "failed"


def test_dlq_persists(temp_db):
    """Test that dead letter queue persists."""
    # Create job and exhaust retries
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        job = repo.create_job(command="false", job_id="dlq-test", max_retries=1)
        repo.mark_failure(job.id, 1, "", "error", 100)
    
    # Check DLQ in new session
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        dlq_jobs = repo.list_dlq_jobs()
        
        assert len(dlq_jobs) == 1
        assert dlq_jobs[0].id == "dlq-test"
        assert dlq_jobs[0].state == "dead"


def test_database_survives_engine_recreation(temp_db):
    """Test that data survives engine recreation."""
    # Create jobs
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        for i in range(10):
            repo.create_job(command=f"echo {i}", job_id=f"survive-{i}")
    
    # Recreate engine
    engine = create_db_engine(temp_db)
    engine.dispose()
    
    # Check data still exists
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        jobs = repo.list_jobs()
        
        assert len(jobs) == 10


def test_indexes_created(temp_db):
    """Test that database indexes are created."""
    from sqlalchemy import inspect
    
    engine = create_db_engine(temp_db)
    inspector = inspect(engine)
    
    # Check jobs table indexes
    indexes = inspector.get_indexes("jobs")
    index_names = [idx["name"] for idx in indexes]
    
    # Should have our custom indexes
    assert any("idx_jobs_polling" in name for name in index_names)
    assert any("idx_jobs_locked_by" in name for name in index_names)


def test_scheduled_jobs_persist_timing(temp_db):
    """Test that scheduled job timing persists."""
    from datetime import timedelta
    from queuectl.utils.time import utcnow
    
    future = utcnow() + timedelta(hours=1)
    
    # Create scheduled job
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        job = repo.create_job(command="echo scheduled", job_id="sched-test", run_at=future)
    
    # Verify timing in new session
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        job = repo.get_job("sched-test")
        
        # Times should match (within 1 second due to precision)
        # Compare as naive datetimes to handle timezone differences
        job_time = job.run_at.replace(tzinfo=None) if job.run_at.tzinfo else job.run_at
        future_time = future.replace(tzinfo=None) if future.tzinfo else future
        time_diff = abs((job_time - future_time).total_seconds())
        assert time_diff < 1
