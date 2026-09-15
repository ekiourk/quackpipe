"""
Tests for opening DuckLake catalogs across DuckDB versions.

DuckLake 1.0 (DuckDB >= 1.5.2) refuses to open catalogs written by older
DuckLake versions unless the ATTACH statement carries AUTOMATIC_MIGRATION, and
older DuckLake extensions reject that option as unknown. quackpipe supports the
DuckDB 1.4 LTS line as well as the current line, so both behaviours are covered
here and the assertions branch on the installed DuckDB.

The fixture ``tests/fixtures/ducklake_v0_3.tar.gz`` is a DuckLake with catalog
version 0.3, created on DuckDB 1.4.5 with the DuckLake extension of that line::

    ATTACH 'ducklake:catalog.duckdb' AS lake (DATA_PATH 'data/');
    CREATE TABLE lake.orders AS SELECT range AS id, 'item_' || range AS item FROM range(10);

It uses a relative DATA_PATH so it can be extracted anywhere; tests chdir into
the extraction directory before attaching.
"""

import tarfile
from pathlib import Path

import duckdb
import pytest

import quackpipe
from quackpipe import QuackpipeBuilder, SourceType
from quackpipe.config import SourceConfig
from quackpipe.exceptions import ConfigError, DuckLakeMigrationError, ValidationError
from quackpipe.sources.ducklake import (
    MIN_DUCKDB_FOR_AUTOMATIC_MIGRATION,
    DuckLakeHandler,
    installed_duckdb_version,
    supports_automatic_migration,
)

LEGACY_LAKE_FIXTURE = Path(__file__).parent / "fixtures" / "ducklake_v0_3.tar.gz"
LEGACY_LAKE_ROWS = 10


def legacy_lake_config(automatic_migration: bool | None = None) -> SourceConfig:
    catalog: dict[str, object] = {"type": "sqlite", "path": "catalog.duckdb"}
    if automatic_migration is not None:
        catalog["automatic_migration"] = automatic_migration
    return SourceConfig(
        name="legacy_lake",
        type=SourceType.DUCKLAKE,
        config={"catalog": catalog, "storage": {"type": "local", "path": "data/"}},
    )


def count_orders(config: SourceConfig) -> int:
    with quackpipe.session(configs=[config]) as con:
        return con.execute("SELECT count(*) FROM legacy_lake.orders").fetchone()[0]


@pytest.fixture
def legacy_lake(tmp_path, monkeypatch) -> Path:
    """Extracts the 0.3 fixture into a temp dir and makes it the working directory."""
    with tarfile.open(LEGACY_LAKE_FIXTURE) as tar:
        tar.extractall(tmp_path, filter="data")
    monkeypatch.chdir(tmp_path)
    return tmp_path


# --- Unit tests: SQL rendering and validation -------------------------------


def test_attach_sql_is_unchanged_when_option_is_not_set():
    handler = DuckLakeHandler(
        {
            "connection_name": "lake",
            "catalog": {"type": "sqlite", "path": "/tmp/catalog.db"},
            "storage": {"type": "local", "path": "/tmp/data/"},
        }
    )
    assert handler.render_sql().endswith("ATTACH 'ducklake:lake_secret' AS lake;")


def test_attach_sql_carries_automatic_migration_when_supported(monkeypatch):
    monkeypatch.setattr(duckdb, "__version__", "1.5.5")
    handler = DuckLakeHandler(
        {
            "connection_name": "lake",
            "catalog": {"type": "sqlite", "path": "/tmp/catalog.db", "automatic_migration": True},
            "storage": {"type": "local", "path": "/tmp/data/"},
        }
    )
    assert handler.render_sql().endswith("ATTACH 'ducklake:lake_secret' AS lake (AUTOMATIC_MIGRATION);")


def test_automatic_migration_false_renders_plain_attach(monkeypatch):
    monkeypatch.setattr(duckdb, "__version__", "1.4.4")
    handler = DuckLakeHandler(
        {
            "connection_name": "lake",
            "catalog": {"type": "sqlite", "path": "/tmp/catalog.db", "automatic_migration": False},
            "storage": {"type": "local", "path": "/tmp/data/"},
        }
    )
    assert handler.render_sql().endswith("ATTACH 'ducklake:lake_secret' AS lake;")


def test_automatic_migration_rejected_on_old_duckdb(monkeypatch):
    monkeypatch.setattr(duckdb, "__version__", "1.4.4")
    config = {
        "catalog": {"type": "sqlite", "path": "/tmp/catalog.db", "automatic_migration": True},
        "storage": {"type": "local", "path": "/tmp/data/"},
    }

    with pytest.raises(ValidationError, match="requires DuckDB >= 1.5.2"):
        QuackpipeBuilder().add_source(name="lake", source_type=SourceType.DUCKLAKE, config=config)

    # render_sql is defensive too, for configs that bypass validation.
    with pytest.raises(ConfigError, match="requires DuckDB >= 1.5.2"):
        DuckLakeHandler({"connection_name": "lake", **config}).render_sql()


@pytest.mark.parametrize(
    "version, expected",
    [
        ("1.4.4", (1, 4, 4)),
        ("1.5.2", (1, 5, 2)),
        ("2.0.0.dev2609121639", (2, 0, 0)),
        ("garbage", (0, 0, 0)),
    ],
)
def test_installed_duckdb_version_parsing(monkeypatch, version, expected):
    monkeypatch.setattr(duckdb, "__version__", version)
    assert installed_duckdb_version() == expected
    assert supports_automatic_migration() == (expected >= MIN_DUCKDB_FOR_AUTOMATIC_MIGRATION)


def test_translate_error_only_handles_version_mismatch():
    handler = DuckLakeHandler(
        {
            "connection_name": "lake",
            "catalog": {"type": "sqlite", "path": "/tmp/catalog.db"},
            "storage": {"type": "local", "path": "/tmp/data/"},
        }
    )
    mismatch = Exception(
        "Invalid Input Error: DuckLake catalog version mismatch: catalog version is 0.3, "
        "but the extension requires version 1.0."
    )
    translated = handler.translate_error(mismatch)
    assert isinstance(translated, DuckLakeMigrationError)
    assert "source 'lake'" in translated.message
    assert "automatic_migration: true" in translated.message
    assert "one-way" in translated.message

    assert handler.translate_error(Exception("IO Error: something else")) is None


# --- Integration tests: real 0.3 catalog against the installed DuckDB -------


@pytest.mark.skipif(supports_automatic_migration(), reason="behaviour of DuckDB < 1.5.2")
def test_legacy_catalog_opens_as_is_on_old_duckdb(legacy_lake):
    assert count_orders(legacy_lake_config()) == LEGACY_LAKE_ROWS


@pytest.mark.skipif(not supports_automatic_migration(), reason="behaviour of DuckDB >= 1.5.2")
def test_legacy_catalog_is_refused_with_guidance_on_new_duckdb(legacy_lake):
    with pytest.raises(DuckLakeMigrationError, match="automatic_migration: true") as exc_info:
        count_orders(legacy_lake_config())
    # The original DuckDB error is preserved for debugging.
    assert isinstance(exc_info.value.__cause__, duckdb.Error)


@pytest.mark.skipif(not supports_automatic_migration(), reason="behaviour of DuckDB >= 1.5.2")
def test_legacy_catalog_migrates_once_when_opted_in(legacy_lake):
    # Migrate in one session ...
    assert count_orders(legacy_lake_config(automatic_migration=True)) == LEGACY_LAKE_ROWS
    # ... after which the option is no longer needed.
    assert count_orders(legacy_lake_config()) == LEGACY_LAKE_ROWS
