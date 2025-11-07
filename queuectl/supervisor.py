"""Supervisor for managing multiple worker processes."""

import json
import multiprocessing
import os
import signal
import time
from multiprocessing import Process
from pathlib import Path
from typing import Optional

from queuectl.db import get_session
from queuectl.repo import WorkerRepository
from queuectl.worker import run_worker

# Required for Windows multiprocessing
if __name__ != "__main__":
    multiprocessing.freeze_support()


class Supervisor:
    """Manages multiple worker processes."""

    def __init__(self, db_path: str | None = None):
        """Initialize supervisor.
        
        Args:
            db_path: Optional database path.
        """
        self.db_path = db_path
        self.pid_file = Path.home() / ".queuectl" / "workers.pid"
        self.pid_file.parent.mkdir(parents=True, exist_ok=True)

    def start_workers(self, count: int) -> list[int]:
        """Start N worker processes.
        
        Args:
            count: Number of workers to start.
            
        Returns:
            List of worker PIDs.
        """
        # Clean up stale workers from database
        with next(get_session(self.db_path)) as session:
            worker_repo = WorkerRepository(session)
            cleaned = worker_repo.cleanup_stale_workers(stale_threshold_s=60)
            if cleaned > 0:
                print(f"Cleaned up {cleaned} stale worker entries")
        
        pids = []
        
        for i in range(count):
            worker_id = f"worker-{i+1}-{os.getpid()}"
            
            # Create worker process (not daemon so it survives parent exit)
            process = Process(
                target=run_worker,
                args=(worker_id, self.db_path),
                name=worker_id,
                daemon=False,
            )
            process.start()
            
            pids.append(process.pid)
            print(f"Started worker {worker_id} (PID: {process.pid})")
            
            # Don't keep references to process objects - let them run independently
            # The process will continue running after parent exits
        
        # Save PIDs to file
        self._save_pids(pids)
        
        return pids

    def stop_workers(self, timeout: int = 30) -> int:
        """Stop all active workers gracefully.
        
        Args:
            timeout: Maximum time to wait for workers to stop.
            
        Returns:
            Number of workers stopped.
        """
        pids = self._load_pids()
        
        if not pids:
            print("No active workers found")
            return 0
        
        stopped = 0
        
        for pid in pids:
            try:
                # Send SIGTERM for graceful shutdown
                if os.name == 'nt':
                    # Windows: Use taskkill
                    import subprocess
                    subprocess.run(['taskkill', '/PID', str(pid), '/T'], check=False)
                    print(f"Sent terminate signal to worker PID {pid}")
                else:
                    os.kill(pid, signal.SIGTERM)
                    print(f"Sent SIGTERM to worker PID {pid}")
                stopped += 1
            except ProcessLookupError:
                print(f"Worker PID {pid} not found (already stopped)")
            except PermissionError:
                print(f"Permission denied to stop worker PID {pid}")
            except Exception as e:
                print(f"Error stopping worker PID {pid}: {e}")
        
        # Wait for processes to terminate
        if stopped > 0:
            print(f"Waiting up to {timeout}s for workers to finish current jobs...")
            
            start_time = time.time()
            remaining_pids = pids.copy()
            
            while remaining_pids and (time.time() - start_time) < timeout:
                for pid in remaining_pids[:]:
                    try:
                        if os.name == 'nt':
                            # Windows: Check if process exists
                            import subprocess
                            result = subprocess.run(
                                ['tasklist', '/FI', f'PID eq {pid}'],
                                capture_output=True,
                                text=True
                            )
                            if str(pid) not in result.stdout:
                                remaining_pids.remove(pid)
                                print(f"Worker PID {pid} stopped")
                        else:
                            os.kill(pid, 0)  # Unix: Check if process exists
                    except ProcessLookupError:
                        remaining_pids.remove(pid)
                        print(f"Worker PID {pid} stopped")
                    except Exception:
                        pass
                
                if remaining_pids:
                    time.sleep(0.5)
            
            # Force kill any remaining processes
            if remaining_pids:
                print(f"Force killing {len(remaining_pids)} workers that didn't stop gracefully")
                for pid in remaining_pids:
                    try:
                        if os.name == 'nt':
                            import subprocess
                            subprocess.run(['taskkill', '/F', '/PID', str(pid)], check=False)
                        else:
                            os.kill(pid, signal.SIGKILL)
                    except (ProcessLookupError, PermissionError):
                        pass
        
        # Clear PID file
        self._clear_pids()
        
        return stopped

    def _save_pids(self, pids: list[int]) -> None:
        """Save worker PIDs to file.
        
        Args:
            pids: List of PIDs to save.
        """
        data = {"pids": pids, "timestamp": time.time()}
        
        with open(self.pid_file, "w") as f:
            json.dump(data, f)

    def _load_pids(self) -> list[int]:
        """Load worker PIDs from file.
        
        Returns:
            List of PIDs, or empty list if file doesn't exist.
        """
        if not self.pid_file.exists():
            return []
        
        try:
            with open(self.pid_file, "r") as f:
                data = json.load(f)
                return data.get("pids", [])
        except (json.JSONDecodeError, IOError):
            return []

    def _clear_pids(self) -> None:
        """Clear PID file."""
        if self.pid_file.exists():
            self.pid_file.unlink()

    def get_worker_status(self) -> dict:
        """Get status of all registered workers.
        
        Returns:
            Dictionary with worker status information.
        """
        pids = self._load_pids()
        running_count = 0
        
        for pid in pids:
            try:
                # On Windows, signal 0 doesn't work, use psutil or try another approach
                if os.name == 'nt':
                    # Windows: Try to open process handle
                    import subprocess
                    result = subprocess.run(
                        ['tasklist', '/FI', f'PID eq {pid}'],
                        capture_output=True,
                        text=True
                    )
                    if str(pid) in result.stdout:
                        running_count += 1
                else:
                    os.kill(pid, 0)  # Unix: Check if process exists
                    running_count += 1
            except (ProcessLookupError, PermissionError, Exception):
                pass
        
        # Get worker info from database
        with next(get_session(self.db_path)) as session:
            worker_repo = WorkerRepository(session)
            active_workers = worker_repo.get_active_workers(stale_threshold_s=10)
        
        return {
            "pids": pids,
            "running_count": running_count,
            "active_workers": len(active_workers),
            "workers": [
                {
                    "id": w.id,
                    "started_at": w.started_at.isoformat(),
                    "last_heartbeat": w.last_heartbeat.isoformat(),
                    "status": w.status,
                }
                for w in active_workers
            ],
        }
