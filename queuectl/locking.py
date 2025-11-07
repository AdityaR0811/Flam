"""Locking utilities for distributed job claiming."""

import random
from datetime import datetime, timedelta

from queuectl.utils.time import utcnow


def is_lock_expired(
    locked_at: datetime | None, lock_timeout_s: int
) -> bool:
    """Check if a job lock has expired.
    
    Args:
        locked_at: When the job was locked.
        lock_timeout_s: Lock timeout in seconds.
        
    Returns:
        True if lock has expired or no lock present.
    """
    if locked_at is None:
        return True
    
    expiry = locked_at + timedelta(seconds=lock_timeout_s)
    return utcnow() >= expiry


def calculate_backoff_delay(
    attempts: int, base: float, max_backoff_s: int
) -> float:
    """Calculate exponential backoff with jitter.
    
    Uses formula: min(max_backoff_s, base^attempts) + random(0, 0.5*base)
    
    Args:
        attempts: Number of retry attempts.
        base: Exponential base for backoff.
        max_backoff_s: Maximum backoff delay in seconds.
        
    Returns:
        Delay in seconds with jitter applied.
    """
    exponential = base ** attempts
    capped = min(exponential, max_backoff_s)
    jitter = random.uniform(0, 0.5 * base)
    return capped + jitter


def get_next_run_at(
    attempts: int, base: float, max_backoff_s: int
) -> datetime:
    """Calculate next run time after failure.
    
    Args:
        attempts: Number of retry attempts.
        base: Exponential base for backoff.
        max_backoff_s: Maximum backoff delay in seconds.
        
    Returns:
        Next scheduled run time.
    """
    delay = calculate_backoff_delay(attempts, base, max_backoff_s)
    return utcnow() + timedelta(seconds=delay)
