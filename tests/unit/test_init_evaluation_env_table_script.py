from scripts.ops import init_evaluation_env_table as script


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
    assert "Target table: env_config_test" in output


def test_production_dry_run_selects_existing_production_table(capsys):
    result = script.init_evaluation_env_table(
        profile="production",
        dry_run=True,
    )

    assert result == 0
    output = capsys.readouterr().out
    assert "Profile: production" in output
    assert "Target table: evaluation_env_config" in output
