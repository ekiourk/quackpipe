"""
Exception classes for quackpipe.
"""

class QuackpipeError(Exception):
    """Base exception for quackpipe."""
    pass

class ConfigError(QuackpipeError):
    """Raised when there's an error with configuration."""
    message = "Configuration error"

class ValidationError(ConfigError):
    """Raised when a specific source configuration fails semantic validation."""
    message = "Validation error"

class ParsingError(ConfigError):
    """Raised when there's an error parsing or merging configuration files."""
    message = "Configuration parsing error"

class ProviderError(QuackpipeError):
    """Base class for errors involving external providers (DBs, Cloud, Secrets)."""
    message = "External provider error"

class SecretError(ProviderError):
    """Raised when there's an error with secret management."""
    message = "Secret management error"

class ConnectionError(ProviderError):
    """Raised when a connection to an external source cannot be established."""
    message = "Connection error"

class ExtensionError(ProviderError):
    """Raised when a DuckDB extension fails to install or load."""
    message = "Extension error"

class ExecutionError(QuackpipeError):
    """Raised when a logic failure occurs during ETL execution."""
    message = "Execution error"

class AccessDeniedError(ExecutionError):
    """Raised when a source is accessed in an unauthorized way (e.g. writing to read-only)."""
    message = "Access denied"
