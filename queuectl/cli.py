"""CLI interface for queuectl using Typer."""

import json
import multiprocessing
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from queuectl.config import ConfigManager
from queuectl.db import get_db_url, get_session, init_db
from queuectl.repo import JobRepository, WorkerRepository
from queuectl.supervisor import Supervisor
from queuectl.utils.time import ensure_utc, to_iso

app = typer.Typer(
    name="queuectl",
    help="CLI-based background job queue with retries, DLQ, and scheduling",
)
console = Console()


# Subcommands
worker_app = typer.Typer(help="Manage worker processes")
dlq_app = typer.Typer(help="Manage dead letter queue")
config_app = typer.Typer(help="Manage configuration")

app.add_typer(worker_app, name="worker")
app.add_typer(dlq_app, name="dlq")
app.add_typer(config_app, name="config")


def get_db_path() -> str:
    """Get database path from environment or default."""
    return str(Path(get_db_url().replace("sqlite:///", "")))


@app.command()
def enqueue(
    job_data: Optional[str] = typer.Argument(None, help="JSON job object or array"),
    file: Optional[Path] = typer.Option(None, "--file", "-f", help="JSON file with job(s)"),
    db_path: Optional[str] = typer.Option(None, envvar="QUEUECTL_DB_PATH"),
) -> None:
    """Enqueue one or more jobs.
    
    Accepts inline JSON or --file with job array.
    Job fields: id (optional), command, priority, run_at, timeout_s, max_retries, backoff_base
    """
    # Initialize DB if needed
    init_db(db_path)
    
    # Load job data
    if file:
        try:
            with open(file) as f:
                data = json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            console.print(f"[red]Error reading file: {e}[/red]")
            sys.exit(1)
    elif job_data:
        try:
            data = json.loads(job_data)
        except json.JSONDecodeError as e:
            console.print(f"[red]Invalid JSON: {e}[/red]")
            sys.exit(1)
    else:
        console.print("[red]Error: Provide job data or --file[/red]")
        sys.exit(1)
    
    # Normalize to list
    jobs = data if isinstance(data, list) else [data]
    
    # Validate and enqueue
    with next(get_session(db_path)) as session:
        repo = JobRepository(session)
        enqueued = []
        
        for job_spec in jobs:
            try:
                # Validate required fields
                if "command" not in job_spec:
                    console.print(f"[red]Error: Job missing 'command' field: {job_spec}[/red]")
                    continue
                
                # Extract fields
                job_id = job_spec.get("id")
                command = job_spec["command"]
                priority = job_spec.get("priority", 0)
                run_at = job_spec.get("run_at")
                timeout_s = job_spec.get("timeout_s")
                max_retries = job_spec.get("max_retries")
                backoff_base = job_spec.get("backoff_base")
                
                # Create job
                job = repo.create_job(
                    command=command,
                    job_id=job_id,
                    priority=priority,
                    run_at=run_at,
                    timeout_s=timeout_s,
                    max_retries=max_retries,
                    backoff_base=backoff_base,
                )
                
                enqueued.append(job)
                console.print(f"[green]✓[/green] Enqueued job {job.id}")
            
            except ValueError as e:
                console.print(f"[red]Error enqueueing job: {e}[/red]")
                continue
            except Exception as e:
                console.print(f"[red]Unexpected error: {e}[/red]")
                continue
        
        if enqueued:
            console.print(f"\n[bold green]Successfully enqueued {len(enqueued)} job(s)[/bold green]")
        else:
            console.print("[red]No jobs were enqueued[/red]")
            sys.exit(1)


@worker_app.command("start")
def worker_start(
    count: int = typer.Option(1, "--count", "-c", help="Number of workers to start"),
    db_path: Optional[str] = typer.Option(None, envvar="QUEUECTL_DB_PATH"),
) -> None:
    """Start N worker processes."""
    if count < 1:
        console.print("[red]Error: Worker count must be >= 1[/red]")
        sys.exit(1)
    
    # Initialize DB if needed
    init_db(db_path)
    
    supervisor = Supervisor(db_path)
    pids = supervisor.start_workers(count)
    
    console.print(f"[bold green]Started {len(pids)} worker(s)[/bold green]")


@worker_app.command("stop")
def worker_stop(
    db_path: Optional[str] = typer.Option(None, envvar="QUEUECTL_DB_PATH"),
) -> None:
    """Stop all running workers gracefully."""
    supervisor = Supervisor(db_path)
    stopped = supervisor.stop_workers()
    
    if stopped > 0:
        console.print(f"[bold green]Stopped {stopped} worker(s)[/bold green]")
    else:
        console.print("[yellow]No workers to stop[/yellow]")


@app.command()
def status(
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    db_path: Optional[str] = typer.Option(None, envvar="QUEUECTL_DB_PATH"),
) -> None:
    """Show system status with job counts, workers, and metrics."""
    # Initialize DB if needed
    init_db(db_path)
    
    with next(get_session(db_path)) as session:
        job_repo = JobRepository(session)
        worker_repo = WorkerRepository(session)
        config_mgr = ConfigManager(session)
        
        # Get statistics
        state_counts = job_repo.get_state_counts()
        oldest_pending_age = job_repo.get_oldest_pending_age()
        avg_duration = job_repo.get_avg_duration()
        active_workers = worker_repo.get_active_workers()
        all_config = config_mgr.get_all()
    
    # Get supervisor status
    supervisor = Supervisor(db_path)
    worker_status = supervisor.get_worker_status()
    
    status_data = {
        "db_path": get_db_path(),
        "job_counts": state_counts,
        "workers": {
            "active": len(active_workers),
            "pids": worker_status["pids"],
        },
        "metrics": {
            "oldest_pending_age_s": oldest_pending_age,
            "avg_duration_ms": avg_duration,
        },
        "config": all_config,
    }
    
    if json_output:
        print(json.dumps(status_data, indent=2, default=str))
    else:
        console.print("\n[bold cyan]QueueCtl Status[/bold cyan]\n")
        console.print(f"Database: {status_data['db_path']}\n")
        
        # Job counts
        console.print("[bold]Job Counts:[/bold]")
        for state, count in state_counts.items():
            console.print(f"  {state}: {count}")
        
        # Workers
        console.print(f"\n[bold]Workers:[/bold]")
        console.print(f"  Active: {status_data['workers']['active']}")
        console.print(f"  PIDs: {status_data['workers']['pids']}")
        
        # Metrics
        console.print(f"\n[bold]Metrics:[/bold]")
        if oldest_pending_age:
            console.print(f"  Oldest pending: {oldest_pending_age:.1f}s ago")
        if avg_duration:
            console.print(f"  Average duration: {avg_duration:.0f}ms")
        
        console.print()


@app.command("list")
def list_jobs(
    state: Optional[str] = typer.Option(None, "--state", "-s", help="Filter by state"),
    limit: Optional[int] = typer.Option(None, "--limit", "-l", help="Maximum jobs to show"),
    pending_ready_only: bool = typer.Option(
        False, "--pending-ready-only", help="Show only pending jobs ready to run"
    ),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    db_path: Optional[str] = typer.Option(None, envvar="QUEUECTL_DB_PATH"),
) -> None:
    """List jobs with optional filters."""
    init_db(db_path)
    
    with next(get_session(db_path)) as session:
        repo = JobRepository(session)
        jobs = repo.list_jobs(state=state, limit=limit, pending_ready_only=pending_ready_only)
    
    if json_output:
        jobs_data = [
            {
                "id": j.id,
                "command": j.command,
                "state": j.state,
                "attempts": j.attempts,
                "max_retries": j.max_retries,
                "priority": j.priority,
                "run_at": to_iso(j.run_at),
                "created_at": to_iso(j.created_at),
                "updated_at": to_iso(j.updated_at),
                "last_exit_code": j.last_exit_code,
                "duration_ms": j.duration_ms,
            }
            for j in jobs
        ]
        print(json.dumps(jobs_data, indent=2))
    else:
        if not jobs:
            console.print("[yellow]No jobs found[/yellow]")
            return
        
        table = Table(title=f"Jobs ({len(jobs)})")
        table.add_column("ID", style="cyan")
        table.add_column("Command", style="white")
        table.add_column("State", style="magenta")
        table.add_column("Attempts", justify="right")
        table.add_column("Priority", justify="right")
        table.add_column("Created", style="dim")
        
        for job in jobs[:50]:  # Limit display to 50
            cmd_preview = (job.command[:40] + "...") if len(job.command) > 40 else job.command
            table.add_row(
                job.id[:12],
                cmd_preview,
                job.state,
                f"{job.attempts}/{job.max_retries}",
                str(job.priority),
                job.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            )
        
        console.print(table)
        
        if len(jobs) > 50:
            console.print(f"\n[dim]... and {len(jobs) - 50} more jobs[/dim]")


@dlq_app.command("list")
def dlq_list(
    limit: Optional[int] = typer.Option(None, "--limit", "-l", help="Maximum jobs to show"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    db_path: Optional[str] = typer.Option(None, envvar="QUEUECTL_DB_PATH"),
) -> None:
    """List dead letter queue jobs."""
    init_db(db_path)
    
    with next(get_session(db_path)) as session:
        repo = JobRepository(session)
        jobs = repo.list_dlq_jobs(limit=limit)
    
    if json_output:
        jobs_data = [
            {
                "id": j.id,
                "command": j.command,
                "attempts": j.attempts,
                "last_exit_code": j.last_exit_code,
                "stderr": j.stderr,
                "created_at": to_iso(j.created_at),
                "updated_at": to_iso(j.updated_at),
            }
            for j in jobs
        ]
        print(json.dumps(jobs_data, indent=2))
    else:
        if not jobs:
            console.print("[green]Dead letter queue is empty[/green]")
            return
        
        table = Table(title=f"Dead Letter Queue ({len(jobs)} jobs)")
        table.add_column("ID", style="cyan")
        table.add_column("Command", style="white")
        table.add_column("Attempts", justify="right")
        table.add_column("Exit Code", justify="right")
        table.add_column("Updated", style="dim")
        
        for job in jobs:
            cmd_preview = (job.command[:40] + "...") if len(job.command) > 40 else job.command
            table.add_row(
                job.id[:12],
                cmd_preview,
                str(job.attempts),
                str(job.last_exit_code or "N/A"),
                job.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
            )
        
        console.print(table)


@dlq_app.command("retry")
def dlq_retry(
    job_id: str = typer.Argument(..., help="Job ID to retry"),
    db_path: Optional[str] = typer.Option(None, envvar="QUEUECTL_DB_PATH"),
) -> None:
    """Retry a job from the dead letter queue."""
    init_db(db_path)
    
    with next(get_session(db_path)) as session:
        repo = JobRepository(session)
        success = repo.retry_dlq_job(job_id)
    
    if success:
        console.print(f"[green]✓[/green] Job {job_id} moved back to pending queue")
    else:
        console.print(f"[red]Error: Job {job_id} not found in DLQ[/red]")
        sys.exit(1)


@config_app.command("get")
def config_get(
    key: Optional[str] = typer.Argument(None, help="Config key (omit to show all)"),
    db_path: Optional[str] = typer.Option(None, envvar="QUEUECTL_DB_PATH"),
) -> None:
    """Get configuration value(s)."""
    init_db(db_path)
    
    with next(get_session(db_path)) as session:
        config_mgr = ConfigManager(session)
        
        if key:
            value = config_mgr.get(key)
            if value is not None:
                print(f"{key}={value}")
            else:
                console.print(f"[red]Key '{key}' not found[/red]")
                sys.exit(1)
        else:
            all_config = config_mgr.get_all()
            for k, v in sorted(all_config.items()):
                print(f"{k}={v}")


@config_app.command("set")
def config_set(
    key: str = typer.Argument(..., help="Config key"),
    value: str = typer.Argument(..., help="Config value"),
    db_path: Optional[str] = typer.Option(None, envvar="QUEUECTL_DB_PATH"),
) -> None:
    """Set configuration value."""
    init_db(db_path)
    
    # Validate known config keys
    valid_keys = {
        "max_retries": int,
        "backoff_base": float,
        "poll_interval_ms": int,
        "lock_timeout_s": int,
        "job_timeout_s": int,
        "max_backoff_s": int,
    }
    
    if key in valid_keys:
        try:
            # Type validation
            valid_keys[key](value)
        except ValueError:
            console.print(f"[red]Error: Invalid value for {key} (expected {valid_keys[key].__name__})[/red]")
            sys.exit(1)
    
    with next(get_session(db_path)) as session:
        config_mgr = ConfigManager(session)
        config_mgr.set(key, value)
    
    console.print(f"[green]✓[/green] Set {key}={value}")


@app.command("logs")
def logs(
    job_id: str = typer.Argument(..., help="Job ID"),
    db_path: Optional[str] = typer.Option(None, envvar="QUEUECTL_DB_PATH"),
) -> None:
    """Show job execution logs (stdout/stderr, exit code, duration)."""
    init_db(db_path)
    
    with next(get_session(db_path)) as session:
        repo = JobRepository(session)
        job = repo.get_job(job_id)
    
    if not job:
        console.print(f"[red]Job {job_id} not found[/red]")
        sys.exit(1)
    
    console.print(f"\n[bold cyan]Job {job.id}[/bold cyan]\n")
    console.print(f"Command: {job.command}")
    console.print(f"State: {job.state}")
    console.print(f"Attempts: {job.attempts}/{job.max_retries}")
    console.print(f"Exit Code: {job.last_exit_code or 'N/A'}")
    console.print(f"Duration: {job.duration_ms or 'N/A'}ms")
    console.print(f"Created: {job.created_at}")
    console.print(f"Updated: {job.updated_at}")
    
    if job.stdout:
        console.print(f"\n[bold]STDOUT:[/bold]\n{job.stdout}")
    
    if job.stderr:
        console.print(f"\n[bold]STDERR:[/bold]\n{job.stderr}")
    
    if not job.stdout and not job.stderr:
        console.print("\n[dim]No output captured[/dim]")


@app.command("init")
def init(
    db_path: Optional[str] = typer.Option(None, envvar="QUEUECTL_DB_PATH"),
) -> None:
    """Initialize database and configuration."""
    init_db(db_path)
    console.print(f"[green]✓[/green] Database initialized at {get_db_path()}")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    app()
