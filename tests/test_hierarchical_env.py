import os

from quackpipe.secrets import EnvSecretProvider


def test_multiple_env_files(tmp_path):
    f1 = tmp_path / ".env.base"
    f2 = tmp_path / ".env.dev"

    with open(f1, "w") as f:
        f.write("MY_VAR=base_value\nSHARED_VAR=shared_base\n")

    with open(f2, "w") as f:
        f.write("MY_VAR=dev_value\n")

    # Initialize provider with list
    provider = EnvSecretProvider(env_file=[str(f1), str(f2)])

    # Check that f2 overrides f1
    assert provider.env_vars.get("MY_VAR") == "dev_value"
    # Check that f1 values persist if not overridden
    assert provider.env_vars.get("SHARED_VAR") == "shared_base"

def test_single_env_file_compat(tmp_path):
    f1 = tmp_path / ".env"
    with open(f1, "w") as f:
        f.write("FOO=bar\n")

    provider = EnvSecretProvider(env_file=str(f1))
    assert provider.env_vars.get("FOO") == "bar"

def test_missing_env_file_warning(caplog):
    import logging
    with caplog.at_level(logging.WARNING):
        EnvSecretProvider(env_file=["non_existent_file"])

    assert "Warning: env_file 'non_existent_file' not found" in caplog.text

def test_env_loading_no_side_effects(tmp_path):
    f = tmp_path / ".env.test"
    with open(f, "w") as file:
        file.write("SIDE_EFFECT_VAR=should_not_leak\n")

    # Ensure variable is not in os.environ initially
    assert "SIDE_EFFECT_VAR" not in os.environ

    provider = EnvSecretProvider(env_file=str(f))

    # Check it exists in provider
    assert provider.env_vars.get("SIDE_EFFECT_VAR") == "should_not_leak"

    # Check it does NOT exist in os.environ
    assert "SIDE_EFFECT_VAR" not in os.environ

def test_env_loading_overrides_system(tmp_path, monkeypatch):
    """
    Verify that env files override system variables (File > System),
    matching the library's documented behavior.
    """
    monkeypatch.setenv("TEST_VAR", "system_value")

    f = tmp_path / ".env.override"
    with open(f, "w") as file:
        file.write("TEST_VAR=file_value\n")

    provider = EnvSecretProvider(env_file=str(f))

    assert provider.env_vars.get("TEST_VAR") == "file_value"
    # System env should remain untouched
    assert os.environ["TEST_VAR"] == "system_value"
