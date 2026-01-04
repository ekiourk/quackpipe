"""
Handles secret management for quackpipe.
"""
import logging
import os

from dotenv import dotenv_values

logger = logging.getLogger(__name__)


class EnvSecretProvider:
    """
    Fetches secrets from environment variables. If env_file(s) are provided
    during initialization, it loads them in order.
    """

    def __init__(self, env_file: str | list[str] | None = None):
        # Start with a copy of the system environment
        self.env_vars = os.environ.copy()

        env_files = []
        if env_file:
            if isinstance(env_file, str):
                env_files = [env_file]
            elif isinstance(env_file, list):
                env_files = env_file

        # We want files to override the system environment to match previous behavior (load_dotenv(override=True)).
        # And later files override earlier files.
        for file_path in env_files:
            if os.path.exists(file_path):
                logger.info("Loading environment variables from: %s", file_path)
                # dotenv_values returns a dict of variables defined in the file.
                file_vars = dotenv_values(dotenv_path=file_path)
                # Filter out None values to avoid inserting non-strings into the environment emulation
                clean_vars = {k: v for k, v in file_vars.items() if v is not None}
                # Update our local dictionary. This does NOT affect os.environ.
                self.env_vars.update(clean_vars)
            else:
                logger.warning("Warning: env_file '%s' not found. Skipping.", file_path)
    def get_raw_secret(self, name: str) -> dict[str, str]:
        """
        Retrieves secrets from the loaded environment variables by prefix.
        Returns a dict where the key is the FULL env var name and value is the secret.
        e.g., {'PROD_DB_USER': 'test_user'}
        """
        prefix = f"{name.upper()}_"
        secrets = {}
        for key, value in self.env_vars.items():
            if key.startswith(prefix):
                secrets[key] = value  # Use the full, original key
        return secrets


# Global variable holding the current secret provider instance.
_provider: EnvSecretProvider | None = None


def _get_provider() -> EnvSecretProvider:
    """
    Internal function to get the current provider, initializing a default
    one if it hasn't been configured yet.
    """
    global _provider
    if _provider is None:
        _provider = EnvSecretProvider()
    return _provider


def configure_secret_provider(env_file: str | list[str] | None = None):
    """
    Initializes or re-initializes the secret provider, optionally loading
    environment file(s).
    """
    global _provider
    _provider = EnvSecretProvider(env_file=env_file)


def fetch_raw_secret_bundle(name: str) -> dict[str, str]:
    """
    Fetches a secret bundle from the configured provider.
    Returns a dictionary with full environment variable names as keys.
    This is primarily used by the CLI for placeholder generation.
    """
    if not name:
        return {}

    return _get_provider().get_raw_secret(name)


def fetch_secret_bundle(name: str) -> dict[str, str]:
    """
    Fetches a secret bundle and normalizes the keys for use by handlers.
    e.g., {'PROD_DB_HOST': 'db.host.com'} becomes {'host': 'db.host.com'}.
    This is the primary function to be used by source handlers.
    """
    if not name:
        return {}

    raw_secrets = fetch_raw_secret_bundle(name)
    normalized_secrets = {}
    prefix = f"{name.upper()}_"

    for key, value in raw_secrets.items():
        if key.startswith(prefix):
            # Creates a clean key like 'host' from 'PROD_DB_HOST'
            normalized_key = key[len(prefix):].lower()
            normalized_secrets[normalized_key] = value

    return normalized_secrets
