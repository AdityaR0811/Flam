"""Configuration management for queuectl."""

from typing import Any

from sqlalchemy.orm import Session

from queuectl.models import Config


class ConfigManager:
    """Manages system configuration stored in database."""

    def __init__(self, session: Session):
        """Initialize config manager.
        
        Args:
            session: Database session.
        """
        self.session = session

    def get(self, key: str, default: str | None = None) -> str | None:
        """Get configuration value.
        
        Args:
            key: Configuration key.
            default: Default value if key not found.
            
        Returns:
            Configuration value or default.
        """
        config = self.session.get(Config, key)
        return config.value if config else default

    def get_int(self, key: str, default: int = 0) -> int:
        """Get integer configuration value.
        
        Args:
            key: Configuration key.
            default: Default value if key not found.
            
        Returns:
            Integer configuration value.
        """
        value = self.get(key)
        return int(value) if value else default

    def get_float(self, key: str, default: float = 0.0) -> float:
        """Get float configuration value.
        
        Args:
            key: Configuration key.
            default: Default value if key not found.
            
        Returns:
            Float configuration value.
        """
        value = self.get(key)
        return float(value) if value else default

    def set(self, key: str, value: Any) -> None:
        """Set configuration value.
        
        Args:
            key: Configuration key.
            value: Configuration value (will be converted to string).
        """
        config = self.session.get(Config, key)
        if config:
            config.value = str(value)
        else:
            config = Config(key=key, value=str(value))
            self.session.add(config)
        self.session.commit()

    def get_all(self) -> dict[str, str]:
        """Get all configuration values.
        
        Returns:
            Dictionary of all config key-value pairs.
        """
        configs = self.session.query(Config).all()
        return {c.key: c.value for c in configs}

    def snapshot(self) -> dict[str, Any]:
        """Get snapshot of current config for job creation.
        
        Returns:
            Dictionary with typed config values.
        """
        return {
            "max_retries": self.get_int("max_retries", 3),
            "backoff_base": self.get_float("backoff_base", 2.0),
            "job_timeout_s": self.get_int("job_timeout_s", 0),
        }
