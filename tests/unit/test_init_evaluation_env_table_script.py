import pyarrow as pa

from scripts.ops import init_evaluation_env_table as script
from wt_sdk.core.evaluation_env_schema import EVALUATION_ENV_SCHEMA


def test_env_schema_includes_nullable_metadata_columns():
    expected = [
        pa.field("description", pa.string(), nullable=True),
        pa.field("primary_category", pa.string(), nullable=True),
        pa.field("secondary_category", pa.string(), nullable=True),
        pa.field("env_meta_data", pa.string(), nullable=True),
        pa.field("is_successed", pa.bool_(), nullable=True),
    ]

    assert list(EVALUATION_ENV_SCHEMA)[-5:] == expected
    assert "is_successed" in script.SCALAR_INDEX_COLUMNS


def test_dry_run_defaults_to_test_table_without_connecting(monkeypatch, capsys):
    monkeypatch.delenv("WT_SDK_PROFILE", raising=False)
    monkeypatch.setattr(
        script.dldb,
        "connect",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("dry-run must not connect")
        ),
    )

    result = script.init_evaluation_env_table(dry_run=True)

    assert result == 0
    output = capsys.readouterr().out
    assert "Profile: test" in output
    assert "Target table: env_test" in output


def test_production_dry_run_selects_existing_production_table(capsys):
    result = script.init_evaluation_env_table(
        profile="production",
        dry_run=True,
    )

    assert result == 0
    output = capsys.readouterr().out
    assert "Profile: production" in output
    assert "Target table: wt_env_config" in output
