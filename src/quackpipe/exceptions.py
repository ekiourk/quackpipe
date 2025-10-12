"""
Exception classes for quackpipe.
"""

class QuackpipeError(Exception):
    """Base exception for quackpipe."""
    pass

class ConfigError(QuackpipeError):
    """Raised when there's an error with configuration."""
    message = "Configuration error"

class SecretError(QuackpipeError):
    """Raised when there's an error with secret management."""
    message = "Secret management error"
