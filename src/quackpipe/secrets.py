"""
Handles secret management for quackpipe.

This module supports a chain of secret providers. The library will
try each provider in order until one successfully returns the requested secret.
"""
import os
import json
from abc import ABC, abstractmethod
from typing import Dict, List, Any
from .exceptions import SecretError

class BaseSecretProvider(ABC):
    """Abstract base class for a secret provider."""
    @abstractmethod
    def get_secret(self, name: str) -> Dict[str, str]:
        """
        Fetch a bundle of secrets for a given logical name.

        Args:
            name: The logical name of the secret bundle (e.g., 'pg_prod').

        Returns:
            A dictionary of key-value pairs for the secret.
            Returns an empty dictionary if the secret is not found by this provider.
        """
        pass

class EnvSecretProvider(BaseSecretProvider):
    """
    Fetches secrets from environment variables.
    Convention: Looks for variables prefixed with the secret name in uppercase,
    e.g., for secret 'pg_prod', it looks for PG_PROD_HOST, PG_PROD_USER, etc.
    """
    def get_secret(self, name: str) -> Dict[str, str]:
        prefix = f"{name.upper()}_"
        secrets = {}
        for key, value in os.environ.items():
            if key.startswith(prefix):
                secret_key = key[len(prefix):].lower()
                secrets[secret_key] = value
        return secrets

class JsonFileSecretProvider(BaseSecretProvider):
    """Fetches secrets from JSON files in a specified directory."""
    def __init__(self, secrets_dir: str = "./secrets"):
        self.secrets_dir = secrets_dir

    def get_secret(self, name: str) -> Dict[str, str]:
        try:
            path = os.path.join(self.secrets_dir, f"{name}.json")
            with open(path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {} # Not found, so another provider can be tried.

# Global list of provider instances.
_providers: List[BaseSecretProvider] = [EnvSecretProvider()]

def set_secret_providers(providers: List[BaseSecretProvider]):
    """
    Sets a custom chain of secret providers.

    Args:
        providers: A list of provider instances. They will be tried in order.
    """
    global _providers
    if not isinstance(providers, list) or not all(isinstance(p, BaseSecretProvider) for p in providers):
        raise TypeError("Argument must be a list of BaseSecretProvider instances.")
    _providers = providers

def fetch_secret_bundle(name: str) -> Dict[str, str]:
    """
    Tries to fetch a secret bundle from the configured providers.
    """
    if not name:
        return {}
        
    for provider in _providers:
        secrets = provider.get_secret(name)
        if secrets:
            return secrets # Return the first one found
    
    raise SecretError(f"Secret bundle '{name}' not found in any configured provider.")
