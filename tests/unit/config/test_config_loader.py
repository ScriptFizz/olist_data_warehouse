from pathlib import Path

from olist_dw.config.config_loader import ConfigLoader


def test_default_configuration_resolves_from_repository_root() -> None:
    loader = ConfigLoader()

    expected_path = Path(__file__).resolve().parents[3] / "settings.yaml"

    assert loader.config_path == expected_path
    assert loader.config_path.is_file()
    assert "paths" in loader.as_dict()
