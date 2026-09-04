from pathlib import Path

import pytest

from olist_dw.config.config_loader import ConfigLoader


def test_default_configuration_resolves_from_repository_root() -> None:
    loader = ConfigLoader()

    expected_path = Path(__file__).resolve().parents[3] / "settings.yaml"

    assert loader.config_path == expected_path
    assert loader.config_path.is_file()
    assert "paths" in loader.as_dict()


def test_project_root_can_be_configured_from_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("paths:\n  raw_data_dir: data/raw\n")
    monkeypatch.setenv("OLIST_PROJECT_ROOT", str(tmp_path))

    loader = ConfigLoader()

    assert loader.project_root == tmp_path.resolve()
    assert loader.config_path == config_path.resolve()
