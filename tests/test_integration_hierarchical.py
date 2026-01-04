
import yaml

from quackpipe.core import get_source_params


def test_session_multiple_configs_integration(tmp_path):
    """
    Integration test to verify that get_source_params (and by extension session)
    correctly loads and merges multiple configuration files.
    """
    # 1. Base config
    base_config = {
        "sources": {
            "source1": {"type": "sqlite", "path": ":memory:"},
            "source2": {"type": "sqlite", "path": "base.db"}
        }
    }
    f1 = tmp_path / "base.yml"
    with open(f1, "w") as f:
        yaml.dump(base_config, f)

    # 2. Override config
    override_config = {
        "sources": {
            "source2": {"path": "override.db"}
        }
    }
    f2 = tmp_path / "override.yml"
    with open(f2, "w") as f:
        yaml.dump(override_config, f)

    # 3. Check source2 (overridden)
    params = get_source_params("source2", config_path=[str(f1), str(f2)])
    assert params["path"] == "override.db"

    # 4. Check source1 (inherited)
    params1 = get_source_params("source1", config_path=[str(f1), str(f2)])
    assert params1["path"] == ":memory:"

def test_session_multiple_env_files_integration(tmp_path):
    """
    Integration test to verify that secrets are correctly loaded and merged
    from multiple environment files.
    """
    # 1. Base env
    f1 = tmp_path / ".env.base"
    with open(f1, "w") as f:
        f.write("MY_SECRET_HOST=base_host\nMY_SECRET_USER=base_user\n")

    # 2. Override env
    f2 = tmp_path / ".env.dev"
    with open(f2, "w") as f:
        f.write("MY_SECRET_HOST=dev_host\n")

    # 3. Config using this secret
    config_data = {
        "sources": {
            "my_source": {
                "type": "postgres",
                "secret_name": "my_secret"
            }
        }
    }
    config_file = tmp_path / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    # 4. Check params
    # Note: We rely on get_source_params calling configure_secret_provider internally
    params = get_source_params(
        "my_source",
        config_path=str(config_file),
        env_file=[str(f1), str(f2)]
    )

    assert params["host"] == "dev_host"
    assert params["user"] == "base_user"

