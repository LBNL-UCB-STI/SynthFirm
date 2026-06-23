from pathlib import Path

from utils.config_paths import resolve_config_path


def test_relative_file_path_resolves_from_config_directory(tmp_path):
    config_dir = tmp_path / "run-home"
    config_dir.mkdir()
    config_file = config_dir / "Austin_local.conf"
    config_file.write_text("[ENVIRONMENT]\nfile_path = .\n", encoding="utf-8")

    resolved = resolve_config_path(".", config_file)

    assert resolved == config_dir.resolve()


def test_environment_variable_file_path_expands_from_config_directory(monkeypatch, tmp_path):
    config_dir = tmp_path / "configs"
    data_root = tmp_path / "data-root"
    config_dir.mkdir()
    data_root.mkdir()
    config_file = config_dir / "Austin_local.conf"
    monkeypatch.setenv("SYNTHFIRM_DATA_ROOT", str(data_root))

    resolved = resolve_config_path("$SYNTHFIRM_DATA_ROOT", config_file)

    assert resolved == data_root.resolve()


def test_absolute_file_path_is_preserved(tmp_path):
    config_file = tmp_path / "configs" / "Austin_local.conf"
    data_root = tmp_path / "data-root"

    resolved = resolve_config_path(data_root, config_file)

    assert resolved == data_root.resolve()
