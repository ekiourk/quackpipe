import os

import pytest
import yaml

from quackpipe.config import get_config_yaml, parse_config_from_yaml
from quackpipe.exceptions import ConfigError


def run_validation_test(temp_dir, invalid_config, should_pass=False):
    config_path = os.path.join(temp_dir, 'config.yml')
    with open(config_path, 'w') as f:
        yaml.dump(invalid_config, f)

    if should_pass:
        parse_config_from_yaml(get_config_yaml(config_path))
    else:
        with pytest.raises(ConfigError):
            parse_config_from_yaml(get_config_yaml(config_path))

@pytest.mark.parametrize("source_config, should_pass", [
    # Valid configs
    ({"type": "postgres", "host": "localhost", "secret_name": "..."}, True),
    ({"type": "mysql", "host": "localhost", "secret_name": "..."}, True),
    ({"type": "s3", "region": "us-east-1", "secret_name": "..."}, True),
    ({"type": "azure", "provider": "connection_string", "secret_name": "..."}, True),
    ({"type": "azure", "provider": "managed_identity"}, True),
    ({"type": "sqlite", "path": "/tmp/db.sqlite"}, True),
    ({"type": "ducklake", "catalog": {"type": "sqlite", "path": "/tmp/cat.db"}, "storage": {"type": "local", "path": "/tmp/store"}}, True),

    # Invalid postgres
    ({"type": "postgres", "port": "not-a-number", "secret_name": "..."}, False),
    ({"type": "postgres", "tables": "not-a-list", "secret_name": "..."}, False),

    # Invalid s3
    ({"type": "s3", "use_ssl": "not-a-boolean", "secret_name": "..."}, False),

    # Invalid azure
    ({"type": "azure", "provider": "service_principal"}, False), # Missing required fields
    ({"type": "azure", "provider": "invalid_provider", "secret_name": "..."}, False),

    # Invalid sqlite
    ({"type": "sqlite", "secret_name": "..."}, False), # Missing path

    # Invalid ducklake
    ({"type": "ducklake", "catalog": {"type": "unsupported"}, "storage": {"type": "local", "path": "/tmp/store"}}, False),
    ({"type": "ducklake", "catalog": {"type": "sqlite", "path": "/tmp/cat.db"}}, False), # Missing storage
])
def test_source_validation(tmpdir, source_config, should_pass):
    """Tests various valid and invalid source configurations."""
    config = {"sources": {"my_source": source_config}}
    run_validation_test(tmpdir, config, should_pass)
