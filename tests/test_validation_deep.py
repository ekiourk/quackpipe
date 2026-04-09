"""
Tests for deep validation (resolve_secrets=True) in quackpipe.
"""

from pathlib import Path

import pytest
import yaml

from quackpipe.config import get_configs, parse_config_from_yaml
from quackpipe.exceptions import ValidationError
from quackpipe.secrets import configure_secret_provider


@pytest.fixture
def simple_pg_config(tmp_path):
    """A config with a postgres source using a secret."""
    config = {"sources": {"pg_test": {"type": "postgres", "secret_name": "my_pg_secret"}}}
    path = tmp_path / "deep_val.yml"
    with path.open("w") as f:
        yaml.dump(config, f)
    return str(path)


def test_inline_postgres_validation_passes(tmp_path):
    """Verify that inline postgres config (no secrets) passes validation."""
    config = {"sources": {"pg_inline": {"type": "postgres", "host": "localhost", "database": "test_db"}}}
    path = tmp_path / "inline_val.yml"
    with path.open("w") as f:
        yaml.dump(config, f)

    # Should not raise
    get_configs(config_path=str(path))


def test_deep_validation_fails_when_secret_missing(simple_pg_config):
    """When resolve_secrets=True, it should fail if env vars are missing."""
    with Path(simple_pg_config).open() as f:
        raw_config = yaml.safe_load(f)

    # Ensure environment is clean for this secret
    configure_secret_provider(env_file=None)

    with pytest.raises(ValidationError, match="requires 'host', 'database'"):
        parse_config_from_yaml(raw_config, resolve_secrets=True)


def test_deep_validation_passes_when_secret_present(simple_pg_config, monkeypatch):
    """When resolve_secrets=True, it should pass if env vars are present."""
    with Path(simple_pg_config).open() as f:
        raw_config = yaml.safe_load(f)

    # Set required secrets
    monkeypatch.setenv("MY_PG_SECRET_HOST", "localhost")
    monkeypatch.setenv("MY_PG_SECRET_DATABASE", "test_db")

    # Refresh provider to see new env vars
    configure_secret_provider(env_file=None)

    # Should not raise
    parse_config_from_yaml(raw_config, resolve_secrets=True)


def test_static_validation_ignores_missing_secrets(simple_pg_config):
    """When resolve_secrets=False (default), missing env vars are ignored."""
    with Path(simple_pg_config).open() as f:
        raw_config = yaml.safe_load(f)

    configure_secret_provider(env_file=None)

    # Should not raise
    parse_config_from_yaml(raw_config, resolve_secrets=False)


def test_get_configs_threads_resolve_secrets(simple_pg_config):
    """Verify that get_configs passes the resolve_secrets flag down."""
    configure_secret_provider(env_file=None)

    # Should fail if resolve_secrets=True is passed to get_configs
    with pytest.raises(ValidationError, match="requires 'host', 'database'"):
        get_configs(config_path=simple_pg_config, resolve_secrets=True)

    # Should pass by default
    get_configs(config_path=simple_pg_config)


def test_caching_prevents_double_fetch(monkeypatch):
    """Verify that fetch_secret_bundle uses the internal cache."""
    from quackpipe.secrets import _bundle_cache, fetch_secret_bundle

    # Clean state
    configure_secret_provider(env_file=None)

    monkeypatch.setenv("CACHE_TEST_HOST", "cached_host")

    # Refresh provider after setenv
    configure_secret_provider(env_file=None)

    # First fetch: should populate cache
    secrets1 = fetch_secret_bundle("cache_test")
    assert secrets1["host"] == "cached_host"
    assert "cache_test" in _bundle_cache

    # Change env var manually (bypass provider)
    monkeypatch.setenv("CACHE_TEST_HOST", "new_host")

    # Second fetch: should return cached value, NOT new env value
    secrets2 = fetch_secret_bundle("cache_test")
    assert secrets2["host"] == "cached_host"

    # Reconfiguring provider should clear cache
    configure_secret_provider(env_file=None)
    assert "cache_test" not in _bundle_cache

    # Third fetch: should get new value
    secrets3 = fetch_secret_bundle("cache_test")
    assert secrets3["host"] == "new_host"
