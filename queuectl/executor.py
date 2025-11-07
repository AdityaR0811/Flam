"""Job executor for running subprocess commands."""

import shlex
import subprocess
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class ExecutionResult:
    """Result of a job execution."""

    exit_code: int
    stdout: str
    stderr: str
    duration_ms: int
    timed_out: bool = False


def execute_job(command: str, timeout_s: int | None = None) -> ExecutionResult:
    """Execute a job command as subprocess.
    
    Args:
        command: Command string to execute.
        timeout_s: Optional timeout in seconds (None = no timeout).
        
    Returns:
        Execution result with output and metrics.
    """
    start_time = time.time()
    
    try:
        # Parse command into argv list for shell-agnostic execution
        # Use shell=True only if command contains shell operators
        use_shell = any(op in command for op in ["|", ">", "<", "&", "&&", "||", ";"])
        
        if use_shell:
            args = command
            shell = True
        else:
            try:
                args = shlex.split(command)
                shell = False
            except ValueError:
                # Fallback to shell if parsing fails
                args = command
                shell = True
        
        # Execute with timeout
        actual_timeout = timeout_s if timeout_s and timeout_s > 0 else None
        
        result = subprocess.run(
            args,
            shell=shell,
            capture_output=True,
            text=True,
            timeout=actual_timeout,
        )
        
        duration_ms = int((time.time() - start_time) * 1000)
        
        return ExecutionResult(
            exit_code=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
            duration_ms=duration_ms,
            timed_out=False,
        )
        
    except subprocess.TimeoutExpired as e:
        duration_ms = int((time.time() - start_time) * 1000)
        
        # Decode captured output if available
        stdout = e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
        stderr = e.stderr.decode() if isinstance(e.stderr, bytes) else (e.stderr or "")
        stderr += f"\n[TIMEOUT after {timeout_s}s]"
        
        return ExecutionResult(
            exit_code=-1,
            stdout=stdout,
            stderr=stderr,
            duration_ms=duration_ms,
            timed_out=True,
        )
        
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        
        return ExecutionResult(
            exit_code=-1,
            stdout="",
            stderr=f"Execution error: {str(e)}",
            duration_ms=duration_ms,
            timed_out=False,
        )
