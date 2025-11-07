"""Utility functions for time handling with UTC enforcement."""

from datetime import datetime, timezone


def utcnow() -> datetime:
    """Return current UTC time as timezone-aware datetime.
    
    Returns:
        datetime: Current time in UTC with timezone info.
    """
    return datetime.now(timezone.utc)


def ensure_utc(dt: datetime | str | None) -> datetime | None:
    """Ensure datetime is UTC timezone-aware.
    
    Args:
        dt: Datetime object, ISO string, or None.
        
    Returns:
        UTC timezone-aware datetime or None.
        
    Raises:
        ValueError: If string is not valid ISO format.
    """
    if dt is None:
        return None
    
    if isinstance(dt, str):
        # Parse ISO format string
        if dt.endswith("Z"):
            dt = dt[:-1] + "+00:00"
        parsed = datetime.fromisoformat(dt)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    
    if dt.tzinfo is None:
        # Assume UTC if naive
        return dt.replace(tzinfo=timezone.utc)
    
    return dt.astimezone(timezone.utc)


def to_iso(dt: datetime | None) -> str | None:
    """Convert datetime to ISO format string.
    
    Args:
        dt: Datetime object or None.
        
    Returns:
        ISO format string or None.
    """
    if dt is None:
        return None
    return dt.isoformat()
