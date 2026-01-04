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
