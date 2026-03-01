"""
Exception classes for quackpipe.
"""


class QuackpipeError(Exception):
    """Base exception for quackpipe."""

    default_message = "An internal error occurred in Quackpipe."

    def __init__(self, message: str | None = None, *args):
        effective_message = message if message is not None else self.default_message
        super().__init__(effective_message, *args)
        self.message = effective_message


class ConfigError(QuackpipeError):
    """Raised when there's an error with configuration."""

    default_message = "Configuration error."


class ValidationError(ConfigError):
    """Raised when a specific source configuration fails semantic validation."""

    default_message = "Validation error."


class ParsingError(ConfigError):
    """Raised when there's an error parsing or merging configuration files."""

    default_message = "Configuration parsing error."


class ProviderError(QuackpipeError):
    """Base class for errors involving external providers (DBs, Cloud, Secrets)."""

    default_message = "External provider error."


class SecretError(ProviderError):
    """Raised when there's an error with secret management."""

    default_message = "Secret management error."


class SourceConnectionError(ProviderError):
    """Raised when a connection to an external source cannot be established."""

    default_message = "Connection error."


class ExtensionError(ProviderError):
    """Raised when a DuckDB extension fails to install or load."""

    default_message = "Extension error."


class ExecutionError(QuackpipeError):
    """Raised when a logic failure occurs during ETL execution."""

    default_message = "Execution error."


class AccessDeniedError(ExecutionError):
    """Raised when a source is accessed in an unauthorized way (e.g. writing to read-only)."""

    default_message = "Access denied."
