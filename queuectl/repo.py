"""Repository layer for database operations."""

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from queuectl.config import ConfigManager
from queuectl.locking import get_next_run_at, is_lock_expired
from queuectl.models import Job, Worker
from queuectl.utils.time import ensure_utc, utcnow


class JobRepository:
    """Repository for job operations."""

    def __init__(self, session: Session):
        """Initialize repository.
        
        Args:
            session: Database session.
        """
        self.session = session
        self.config = ConfigManager(session)

    def create_job(
        self,
        command: str,
        job_id: str | None = None,
        priority: int = 0,
        run_at: datetime | str | None = None,
        timeout_s: int | None = None,
        max_retries: int | None = None,
        backoff_base: float | None = None,
    ) -> Job:
        """Create a new job with configuration snapshot.
        
        Args:
            command: Command to execute.
            job_id: Optional job ID (generated if None).
            priority: Job priority (higher = more important).
            run_at: When to run the job (default: now).
            timeout_s: Job-specific timeout override.
            max_retries: Max retry attempts override.
            backoff_base: Backoff base override.
            
        Returns:
            Created job.
            
        Raises:
            ValueError: If job ID already exists.
        """
        if job_id is None:
            job_id = str(uuid.uuid4())
        
        # Check for duplicate ID
        if self.session.get(Job, job_id):
            raise ValueError(f"Job with ID '{job_id}' already exists")
        
        # Get config snapshot
        snapshot = self.config.snapshot()
        
        job = Job(
            id=job_id,
            command=command,
            state="pending",
            attempts=0,
            max_retries=max_retries if max_retries is not None else snapshot["max_retries"],
            backoff_base=backoff_base if backoff_base is not None else snapshot["backoff_base"],
            priority=priority,
            run_at=ensure_utc(run_at) if run_at else utcnow(),
            timeout_s=timeout_s,
            created_at=utcnow(),
            updated_at=utcnow(),
        )
        
        self.session.add(job)
        self.session.commit()
        self.session.refresh(job)
        
        return job

    def claim_job(self, worker_id: str) -> Optional[Job]:
        """Atomically claim an eligible job for processing.
        
        Uses SELECT FOR UPDATE to prevent race conditions.
        
        Args:
            worker_id: ID of the worker claiming the job.
            
        Returns:
            Claimed job or None if no eligible jobs.
        """
        lock_timeout_s = self.config.get_int("lock_timeout_s", 300)
        now = utcnow()
        
        # Find eligible job with locking
        stmt = (
            select(Job)
            .where(
                and_(
                    Job.state.in_(["pending", "failed"]),
                    Job.run_at <= now,
                    or_(
                        Job.locked_by.is_(None),
                        Job.locked_at.is_(None),
                        Job.locked_at < now - timedelta(seconds=lock_timeout_s),
                    ),
                )
            )
            .order_by(Job.run_at.asc(), Job.priority.desc(), Job.created_at.asc())
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        
        job = self.session.scalars(stmt).first()
        
        if job:
            job.locked_by = worker_id
            job.locked_at = now
            job.state = "processing"
            job.updated_at = now
            self.session.commit()
            self.session.refresh(job)
        
        return job

    def mark_success(
        self, job_id: str, exit_code: int, stdout: str, stderr: str, duration_ms: int
    ) -> None:
        """Mark job as completed successfully.
        
        Args:
            job_id: Job ID.
            exit_code: Process exit code.
            stdout: Captured stdout (truncated).
            stderr: Captured stderr (truncated).
            duration_ms: Execution duration in milliseconds.
        """
        job = self.session.get(Job, job_id)
        if not job:
            return
        
        job.state = "completed"
        job.last_exit_code = exit_code
        job.stdout = stdout[:8192] if stdout else None  # Truncate to 8KB
        job.stderr = stderr[:8192] if stderr else None
        job.duration_ms = duration_ms
        job.locked_by = None
        job.locked_at = None
        job.updated_at = utcnow()
        
        self.session.commit()

    def mark_failure(
        self, job_id: str, exit_code: int, stdout: str, stderr: str, duration_ms: int
    ) -> None:
        """Mark job as failed and schedule retry or move to DLQ.
        
        Args:
            job_id: Job ID.
            exit_code: Process exit code.
            stdout: Captured stdout (truncated).
            stderr: Captured stderr (truncated).
            duration_ms: Execution duration in milliseconds.
        """
        job = self.session.get(Job, job_id)
        if not job:
            return
        
        job.attempts += 1
        job.last_exit_code = exit_code
        job.stdout = stdout[:8192] if stdout else None
        job.stderr = stderr[:8192] if stderr else None
        job.duration_ms = duration_ms
        job.updated_at = utcnow()
        
        if job.attempts >= job.max_retries:
            # Move to dead letter queue
            job.state = "dead"
            job.locked_by = None
            job.locked_at = None
        else:
            # Schedule retry with backoff
            max_backoff_s = self.config.get_int("max_backoff_s", 3600)
            job.state = "failed"
            job.run_at = get_next_run_at(job.attempts, job.backoff_base, max_backoff_s)
            job.locked_by = None
            job.locked_at = None
        
        self.session.commit()

    def get_job(self, job_id: str) -> Optional[Job]:
        """Get job by ID.
        
        Args:
            job_id: Job ID.
            
        Returns:
            Job or None.
        """
        return self.session.get(Job, job_id)

    def list_jobs(
        self,
        state: str | None = None,
        limit: int | None = None,
        pending_ready_only: bool = False,
    ) -> list[Job]:
        """List jobs with optional filters.
        
        Args:
            state: Filter by state.
            limit: Maximum number of jobs to return.
            pending_ready_only: Only return pending jobs ready to run.
            
        Returns:
            List of jobs.
        """
        stmt = select(Job)
        
        if state:
            stmt = stmt.where(Job.state == state)
        
        if pending_ready_only:
            stmt = stmt.where(Job.state == "pending", Job.run_at <= utcnow())
        
        stmt = stmt.order_by(Job.created_at.desc())
        
        if limit:
            stmt = stmt.limit(limit)
        
        return list(self.session.scalars(stmt).all())

    def list_dlq_jobs(self, limit: int | None = None) -> list[Job]:
        """List dead letter queue jobs.
        
        Args:
            limit: Maximum number of jobs to return.
            
        Returns:
            List of dead jobs.
        """
        return self.list_jobs(state="dead", limit=limit)

    def retry_dlq_job(self, job_id: str) -> bool:
        """Retry a job from the dead letter queue.
        
        Args:
            job_id: Job ID.
            
        Returns:
            True if job was retried, False if not found or not dead.
        """
        job = self.session.get(Job, job_id)
        if not job or job.state != "dead":
            return False
        
        job.state = "pending"
        job.attempts = 0
        job.run_at = utcnow()
        job.locked_by = None
        job.locked_at = None
        job.updated_at = utcnow()
        
        self.session.commit()
        return True

    def get_state_counts(self) -> dict[str, int]:
        """Get count of jobs by state.
        
        Returns:
            Dictionary mapping state to count.
        """
        from sqlalchemy import func
        
        results = self.session.query(Job.state, func.count(Job.id)).group_by(Job.state).all()
        return {state: count for state, count in results}

    def get_oldest_pending_age(self) -> float | None:
        """Get age of oldest pending job in seconds.
        
        Returns:
            Age in seconds or None if no pending jobs.
        """
        stmt = (
            select(Job.created_at)
            .where(Job.state == "pending")
            .order_by(Job.created_at.asc())
            .limit(1)
        )
        
        oldest = self.session.scalars(stmt).first()
        if oldest:
            # Ensure oldest is timezone-aware
            oldest_utc = ensure_utc(oldest)
            delta = utcnow() - oldest_utc
            return delta.total_seconds()
        return None

    def get_avg_duration(self) -> float | None:
        """Get average job duration in milliseconds.
        
        Returns:
            Average duration or None.
        """
        from sqlalchemy import func
        
        result = self.session.query(func.avg(Job.duration_ms)).filter(
            Job.duration_ms.isnot(None)
        ).scalar()
        
        return float(result) if result else None


class WorkerRepository:
    """Repository for worker operations."""

    def __init__(self, session: Session):
        """Initialize repository.
        
        Args:
            session: Database session.
        """
        self.session = session

    def register_worker(self, worker_id: str) -> Worker:
        """Register a new worker.
        
        Args:
            worker_id: Unique worker ID.
            
        Returns:
            Created worker.
        """
        now = utcnow()
        worker = Worker(
            id=worker_id,
            started_at=now,
            last_heartbeat=now,
            status="active",
        )
        
        self.session.add(worker)
        self.session.commit()
        self.session.refresh(worker)
        
        return worker

    def heartbeat(self, worker_id: str) -> None:
        """Update worker heartbeat.
        
        Args:
            worker_id: Worker ID.
        """
        worker = self.session.get(Worker, worker_id)
        if worker:
            worker.last_heartbeat = utcnow()
            self.session.commit()

    def deregister_worker(self, worker_id: str) -> None:
        """Deregister a worker.
        
        Args:
            worker_id: Worker ID.
        """
        worker = self.session.get(Worker, worker_id)
        if worker:
            self.session.delete(worker)
            self.session.commit()

    def get_active_workers(self, stale_threshold_s: int = 10) -> list[Worker]:
        """Get list of active workers based on recent heartbeats.
        
        Args:
            stale_threshold_s: Seconds without heartbeat to consider stale.
            
        Returns:
            List of active workers.
        """
        from datetime import timedelta
        
        threshold = utcnow() - timedelta(seconds=stale_threshold_s)
        stmt = select(Worker).where(
            Worker.status == "active", Worker.last_heartbeat >= threshold
        )
        
        return list(self.session.scalars(stmt).all())

    def cleanup_stale_workers(self, stale_threshold_s: int = 60) -> int:
        """Remove stale worker entries.
        
        Args:
            stale_threshold_s: Seconds without heartbeat to consider stale.
            
        Returns:
            Number of workers cleaned up.
        """
        from datetime import timedelta
        
        threshold = utcnow() - timedelta(seconds=stale_threshold_s)
        stmt = select(Worker).where(Worker.last_heartbeat < threshold)
        
        stale_workers = list(self.session.scalars(stmt).all())
        for worker in stale_workers:
            self.session.delete(worker)
        
        self.session.commit()
        return len(stale_workers)


# Import timedelta at the top of the file
from datetime import timedelta
