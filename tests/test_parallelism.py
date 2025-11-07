"""Tests for parallel worker execution."""

import tempfile
import time
from multiprocessing import Process
from pathlib import Path

import pytest

from queuectl.db import get_session, init_db
from queuectl.repo import JobRepository
from queuectl.worker import run_worker


@pytest.fixture
def temp_db():
    """Create temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    init_db(db_path)
    yield db_path
    
    Path(db_path).unlink(missing_ok=True)


def test_single_worker_processes_jobs(temp_db):
    """Test that a single worker can process jobs."""
    # Enqueue jobs
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        for i in range(5):
            repo.create_job(command=f"echo test{i}", job_id=f"job-{i}")
    
    # Start worker in subprocess
    worker_process = Process(target=run_worker, args=(f"test-worker", temp_db))
    worker_process.start()
    
    # Wait for processing
    time.sleep(3)
    
    # Stop worker
    worker_process.terminate()
    worker_process.join(timeout=5)
    
    # Check results
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        completed = repo.list_jobs(state="completed")
        
        assert len(completed) > 0


def test_multiple_workers_no_duplicate_processing(temp_db):
    """Test that multiple workers don't process the same job."""
    # Enqueue jobs with varying priorities
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        job_ids = []
        for i in range(20):
            priority = i % 3
            job = repo.create_job(
                command=f"echo job{i}",
                job_id=f"job-{i}",
                priority=priority,
            )
            job_ids.append(job.id)
    
    # Start multiple workers
    workers = []
    for i in range(4):
        worker = Process(target=run_worker, args=(f"worker-{i}", temp_db))
        worker.start()
        workers.append(worker)
    
    # Wait for processing
    time.sleep(5)
    
    # Stop workers
    for worker in workers:
        worker.terminate()
        worker.join(timeout=5)
    
    # Verify no duplicate processing
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        # Get all completed jobs
        completed = repo.list_jobs(state="completed")
        
        # Check for duplicates (should not happen)
        completed_ids = [j.id for j in completed]
        assert len(completed_ids) == len(set(completed_ids)), "Duplicate processing detected!"
        
        # All jobs should be in completed or processing state
        for job_id in job_ids:
            job = repo.get_job(job_id)
            assert job.state in ["completed", "processing", "pending"]


def test_worker_respects_priority(temp_db):
    """Test that workers process higher priority jobs first."""
    # Enqueue jobs with different priorities
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        
        # Low priority jobs
        for i in range(5):
            repo.create_job(command=f"echo low{i}", job_id=f"low-{i}", priority=0)
        
        # High priority job
        repo.create_job(command="echo high", job_id="high-1", priority=100)
    
    # Start worker
    worker_process = Process(target=run_worker, args=("test-worker", temp_db))
    worker_process.start()
    
    # Wait a bit
    time.sleep(2)
    
    # Stop worker
    worker_process.terminate()
    worker_process.join(timeout=5)
    
    # High priority job should be processed first
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        high_job = repo.get_job("high-1")
        
        # High priority job should be completed or processing
        assert high_job.state in ["completed", "processing"]


def test_worker_claims_job_atomically(temp_db):
    """Test that job claiming is atomic (no race conditions)."""
    # Enqueue one job
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        repo.create_job(command="sleep 1", job_id="atomic-test")
    
    # Try to claim with two "workers" simultaneously
    with next(get_session(temp_db)) as session1:
        with next(get_session(temp_db)) as session2:
            repo1 = JobRepository(session1)
            repo2 = JobRepository(session2)
            
            # Both try to claim
            job1 = repo1.claim_job("worker-1")
            job2 = repo2.claim_job("worker-2")
            
            # Only one should succeed
            claims = [j for j in [job1, job2] if j is not None]
            assert len(claims) == 1, "Multiple workers claimed the same job!"


def test_lock_expiry_reclaims_jobs(temp_db):
    """Test that expired locks are reclaimed."""
    from queuectl.config import ConfigManager
    
    # Set short lock timeout for testing
    with next(get_session(temp_db)) as session:
        config = ConfigManager(session)
        config.set("lock_timeout_s", 2)
        
        repo = JobRepository(session)
        job = repo.create_job(command="echo test", job_id="lock-test")
        
        # Claim job
        claimed = repo.claim_job("worker-1")
        assert claimed is not None
    
    # Wait for lock to expire
    time.sleep(3)
    
    # Try to claim again with different worker
    with next(get_session(temp_db)) as session:
        repo = JobRepository(session)
        reclaimed = repo.claim_job("worker-2")
        
        # Should be able to reclaim
        assert reclaimed is not None
        assert reclaimed.id == "lock-test"
        assert reclaimed.locked_by == "worker-2"


def test_worker_heartbeat(temp_db):
    """Test that worker sends heartbeats."""
    from queuectl.repo import WorkerRepository
    
    # Start worker
    worker_process = Process(target=run_worker, args=("heartbeat-worker", temp_db))
    worker_process.start()
    
    # Wait for worker to register and send heartbeat
    time.sleep(3)
    
    # Check heartbeat
    with next(get_session(temp_db)) as session:
        worker_repo = WorkerRepository(session)
        active_workers = worker_repo.get_active_workers(stale_threshold_s=10)
        
        # Should have at least one active worker
        assert len(active_workers) > 0
    
    # Stop worker
    worker_process.terminate()
    worker_process.join(timeout=5)
