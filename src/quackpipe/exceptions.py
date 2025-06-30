"""
Exception classes for quackpipe.
"""

class QuackpipeError(Exception):
    """Base exception for quackpipe."""
    pass

class ConfigError(QuackpipeError):
    """Raised when there's an error with configuration."""
    pass

class SecretError(QuackpipeError):
    """Raised when there's an error with secret management."""
    pass
