
import os

import yaml

from quackpipe.config import deep_merge, get_config_yaml


def test_deep_merge():
    base = {
        "a": 1,
        "b": {"c": 2, "d": 3},
        "list": [1, 2]
    }
    override = {
        "b": {"c": 99},
        "list": [3, 4],
        "new": "value"
    }

    merged = deep_merge(base, override)

    assert merged["a"] == 1
    assert merged["b"]["c"] == 99
    assert merged["b"]["d"] == 3
    assert merged["list"] == [3, 4]  # Lists are replaced
    assert merged["new"] == "value"

def test_load_multiple_configs(tmp_path):
    base_config = {
        "sources": {
            "source1": {"type": "postgres", "secret_name": "s1"}
        },
        "before_all_statements": ["SELECT 1"]
    }

    dev_config = {
        "sources": {
            "source1": {"secret_name": "s1_dev"},
            "source2": {"type": "sqlite", "path": "test.db"}
        },
        "before_all_statements": ["SELECT 2"]
    }

    f1 = tmp_path / "base.yml"
    f2 = tmp_path / "dev.yml"

    with open(f1, "w") as f:
        yaml.dump(base_config, f)
    with open(f2, "w") as f:
        yaml.dump(dev_config, f)

    merged = get_config_yaml([str(f1), str(f2)])

    assert merged["sources"]["source1"]["type"] == "postgres"  # From base
    assert merged["sources"]["source1"]["secret_name"] == "s1_dev"  # Overridden
    assert merged["sources"]["source2"]["type"] == "sqlite"  # New
    assert merged["before_all_statements"] == ["SELECT 2"]  # Replaced

def test_config_env_var(tmp_path, monkeypatch):
    base_config = {"foo": "bar"}
    dev_config = {"foo": "baz"}

    f1 = tmp_path / "c1.yml"
    f2 = tmp_path / "c2.yml"

    with open(f1, "w") as f:
        yaml.dump(base_config, f)
    with open(f2, "w") as f:
        yaml.dump(dev_config, f)

    path_str = f"{f1}{os.pathsep}{f2}"
    monkeypatch.setenv("QUACKPIPE_CONFIG_PATH", path_str)

    merged = get_config_yaml(None)
    assert merged["foo"] == "baz"
