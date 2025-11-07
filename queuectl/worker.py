"""Worker process for executing jobs."""

import os
import signal
import time
import uuid
from typing import Optional

from queuectl.config import ConfigManager
from queuectl.db import get_session
from queuectl.executor import execute_job
from queuectl.logging_conf import setup_logging
from queuectl.models import Job
from queuectl.repo import JobRepository, WorkerRepository


class Worker:
    """Worker process that claims and executes jobs."""

    def __init__(self, worker_id: str | None = None, db_path: str | None = None):
        """Initialize worker.
        
        Args:
            worker_id: Unique worker ID (generated if None).
            db_path: Optional database path.
        """
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.db_path = db_path
        self.running = True
        self.logger = setup_logging(self.worker_id)
        
        # Setup signal handlers (Windows has limited signal support)
        if os.name == 'nt':
            # Windows only supports SIGINT (Ctrl+C) and SIGBREAK
            signal.signal(signal.SIGINT, self._handle_shutdown)
            if hasattr(signal, 'SIGBREAK'):
                signal.signal(signal.SIGBREAK, self._handle_shutdown)
        else:
            # Unix-like systems support SIGTERM and SIGINT
            signal.signal(signal.SIGTERM, self._handle_shutdown)
            signal.signal(signal.SIGINT, self._handle_shutdown)
        
        self.logger.info(f"Worker {self.worker_id} initialized")

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals gracefully.
        
        Args:
            signum: Signal number.
            frame: Current stack frame.
        """
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown")
        self.running = False

    def run(self) -> None:
        """Main worker loop: register, poll, process, heartbeat."""
        self.logger.info(f"Worker {self.worker_id} starting")
        
        # Register worker
        with next(get_session(self.db_path)) as session:
            worker_repo = WorkerRepository(session)
            worker_repo.register_worker(self.worker_id)
        
        try:
            last_heartbeat = time.time()
            heartbeat_interval = 2.0  # seconds
            
            while self.running:
                # Get config for this iteration
                with next(get_session(self.db_path)) as session:
                    config = ConfigManager(session)
                    poll_interval_ms = config.get_int("poll_interval_ms", 500)
                    poll_interval = poll_interval_ms / 1000.0
                
                # Try to claim and process a job
                job = self._claim_job()
                
                if job:
                    self._process_job(job)
                else:
                    # No job available, sleep
                    time.sleep(poll_interval)
                
                # Send heartbeat periodically
                if time.time() - last_heartbeat >= heartbeat_interval:
                    self._send_heartbeat()
                    last_heartbeat = time.time()
        
        finally:
            # Deregister worker
            self.logger.info(f"Worker {self.worker_id} shutting down")
            with next(get_session(self.db_path)) as session:
                worker_repo = WorkerRepository(session)
                worker_repo.deregister_worker(self.worker_id)

    def _claim_job(self) -> Optional[Job]:
        """Attempt to claim an eligible job.
        
        Returns:
            Claimed job or None.
        """
        with next(get_session(self.db_path)) as session:
            repo = JobRepository(session)
            job = repo.claim_job(self.worker_id)
            
            if job:
                self.logger.info(f"Claimed job {job.id}")
            
            return job

    def _process_job(self, job: Job) -> None:
        """Execute a claimed job and update its state.
        
        Args:
            job: Job to process.
        """
        self.logger.info(f"Processing job {job.id}: {job.command}")
        
        try:
            # Determine effective timeout
            timeout_s = job.timeout_s
            if timeout_s is None or timeout_s == 0:
                with next(get_session(self.db_path)) as session:
                    config = ConfigManager(session)
                    global_timeout = config.get_int("job_timeout_s", 0)
                    timeout_s = global_timeout if global_timeout > 0 else None
            
            # Execute job
            result = execute_job(job.command, timeout_s)
            
            # Update job based on result
            with next(get_session(self.db_path)) as session:
                repo = JobRepository(session)
                
                if result.exit_code == 0:
                    self.logger.info(
                        f"Job {job.id} completed successfully in {result.duration_ms}ms"
                    )
                    repo.mark_success(
                        job.id,
                        result.exit_code,
                        result.stdout,
                        result.stderr,
                        result.duration_ms,
                    )
                else:
                    self.logger.warning(
                        f"Job {job.id} failed with exit code {result.exit_code}"
                    )
                    repo.mark_failure(
                        job.id,
                        result.exit_code,
                        result.stdout,
                        result.stderr,
                        result.duration_ms,
                    )
        
        except Exception as e:
            self.logger.error(f"Error processing job {job.id}: {e}", exc_info=True)
            
            # Mark as failed with error info
            with next(get_session(self.db_path)) as session:
                repo = JobRepository(session)
                repo.mark_failure(
                    job.id,
                    -1,
                    "",
                    f"Worker error: {str(e)}",
                    0,
                )

    def _send_heartbeat(self) -> None:
        """Send heartbeat to update worker status."""
        try:
            with next(get_session(self.db_path)) as session:
                worker_repo = WorkerRepository(session)
                worker_repo.heartbeat(self.worker_id)
        except Exception as e:
            self.logger.error(f"Failed to send heartbeat: {e}")


def run_worker(worker_id: str | None = None, db_path: str | None = None) -> None:
    """Run a worker process.
    
    Args:
        worker_id: Optional worker ID.
        db_path: Optional database path.
    """
    worker = Worker(worker_id, db_path)
    worker.run()
