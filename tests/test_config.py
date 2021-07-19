"""Tests for the config classes in config.py."""
import yaml
import pytest

from cprices.config import *
from cprices import config


def write_config_yaml(dir, yaml_input: str) -> None:
    """Write a file called my_config.yaml with given yaml at given dir."""
    with open(dir.join('my_config.yaml'), 'w') as f:
        yaml.dump(yaml.safe_load(yaml_input), f)


@pytest.fixture
def test_config(tmpdir, monkeypatch):
    """Sets up a test config file in tmpdir/config with given yaml."""
    def _(yaml_input: str = "template_text") -> Config:
        config_dir = tmpdir.mkdir('config')
        monkeypatch.setattr(config.Path, 'home', lambda: Path(tmpdir))
        write_config_yaml(config_dir, yaml_input=yaml_input)
        return Config('my_config')
    return _


class TestConfig:
    """Group of tests for Config."""

    def test_init_assigns_name_and_config_path_attrs(self, test_config, tmpdir):
        """Test that the name and config attributes are assigned after init."""
        # test_config creates a config file 'my_config.yaml' in the dir
        # tmpdir/config.
        conf = test_config()
        assert conf.name == 'my_config'
        assert conf.config_path == Path(tmpdir).joinpath('config', 'my_config.yaml')

    def test_get_config_dir_when_cprices_config_env_variable_set(
        self, tmpdir, monkeypatch
    ):
        """Patch the CPRICES_CONFIG env variable and assert get_config_dir method
        returns the locations given by the env var.
        """
        monkeypatch.setenv('CPRICES_CONFIG', str(tmpdir))
        write_config_yaml(tmpdir, yaml_input="template_text")
        conf = Config('my_config')
        assert conf.get_config_dir() == tmpdir

    def test_get_config_dir_returns_cprices_cprices_first_if_exists(
        self, tmpdir, monkeypatch,
    ):
        """Test that get_config_dir returns the config dir in the nested
        folder cprices/cprices ahead of returning a config dir in the current
        working directory or the home locations. This is how the current nesting
        is on DAP.
        """
        target_dir = tmpdir.mkdir('cprices').mkdir('cprices').mkdir('config')
        tmpdir.mkdir('config')

        monkeypatch.setattr(config.Path, 'cwd', lambda: Path(tmpdir))
        monkeypatch.setattr(config.Path, 'home', lambda: Path(tmpdir))

        write_config_yaml(target_dir, yaml_input="template_text")

        assert Config('my_config').get_config_dir() == target_dir

    def test_get_config_dir_returns_home_dir_config_if_no_cprices_dir(
        self, tmpdir, monkeypatch,
    ):
        """Test that get_config_dir returns home before current working directory."""
        target_dir = tmpdir.mkdir('config')

        monkeypatch.setattr(config.Path, 'cwd', lambda: Path(tmpdir))
        monkeypatch.setattr(config.Path, 'home', lambda: Path(tmpdir))

        write_config_yaml(target_dir, yaml_input="template_text")

        assert Config('my_config').get_config_dir() == target_dir

    def test_get_config_path(self, test_config, tmpdir):
        """Test get_config_path returns the path of the given config file."""
        # test_config creates a config file 'my_config.yaml' in the dir
        # tmpdir/config.
        conf = test_config()
        expected = Path(tmpdir).joinpath('config', 'my_config.yaml')
        assert conf.get_config_path() == expected

    def test_load_config(self, test_config):
        """Test load_config method loads YAML input and returns a dict."""
        conf = test_config(yaml_input="""
        bells:
            big_ben:
                dongs: 12
        whistles:
            - referee
            - dog
        """)
        print(conf.load_config())
        assert conf.load_config() == {
            'bells': {'big_ben': {'dongs': 12}},
            'whistles': ['referee', 'dog'],
        }

    @pytest.mark.skip(reason="test shell")
    def test_update(self):
        """Test for this."""
        pass


class TestSelectedScenarioConfig:
    """Group of tests for SelectedScenarioConfig."""

    @pytest.mark.skip(reason="test shell")
    def test_init(self):
        """Test for SelectedScenarioConfig."""
        pass


class TestScenarioConfig:
    """Group of tests for ScenarioConfig."""

    @pytest.mark.skip(reason="test shell")
    def test_init(self):
        """Test for ScenarioConfig."""
        pass

    @pytest.mark.skip(reason="test shell")
    def test_validate(self):
        """Test for this."""
        pass


class TestDevConfig:
    """Group of tests for DevConfig."""

    @pytest.mark.skip(reason="test shell")
    def test_init(self):
        """Test for DevConfig."""
        pass


class TestLoggingConfig:
    """Group of tests for LoggingConfig."""

    @pytest.mark.skip(reason="test shell")
    def test_init(self):
        """Test for LoggingConfig."""
        pass

    @pytest.mark.skip(reason="test shell")
    def test_create_log_id(self):
        """Test for this."""
        pass

    @pytest.mark.skip(reason="test shell")
    def test_get_logs_dir(self):
        """Test for this."""
        pass

    @pytest.mark.skip(reason="test shell")
    def test_create_logs_dir(self):
        """Test for this."""
        pass

    @pytest.mark.skip(reason="test shell")
    def test_set_logging_config(self):
        """Test for this."""
        pass
