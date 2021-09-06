"""Tests for the config classes in config.py."""
import yaml
import pytest

from cprices.config import *
from cprices import config

from tests.conftest import (
    Case,
    parametrize_cases,
)


def write_config_yaml(
    dir,
    yaml_input: str = "my_attr: test",
    name: str = 'my_config',
) -> None:
    """Write a file called my_config.yaml with given yaml at given dir."""
    with open(dir.join(f'{name}.yaml'), 'w') as f:
        yaml.dump(yaml.safe_load(yaml_input), f)


@pytest.fixture
def test_config(tmpdir, monkeypatch):
    """Sets up a test config file in tmpdir/config with given yaml."""
    def _(
        yaml_input: str = "my_attr: test",
        name: str = 'my_config',
    ) -> Config:
        config_dir = (
            tmpdir.join('config') if tmpdir.join('config').check()
            else tmpdir.mkdir('config')
        )
        monkeypatch.setattr(config.Path, 'home', lambda: Path(tmpdir))
        write_config_yaml(config_dir, yaml_input, name)
        return Config(name)
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
        write_config_yaml(tmpdir)
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

        write_config_yaml(target_dir)

        assert Config('my_config').get_config_dir() == target_dir

    def test_get_config_dir_returns_home_dir_config_if_no_cprices_dir(
        self, tmpdir, monkeypatch,
    ):
        """Test that get_config_dir returns home before current working directory."""
        target_dir = tmpdir.mkdir('config')

        monkeypatch.setattr(config.Path, 'cwd', lambda: Path(tmpdir))
        monkeypatch.setattr(config.Path, 'home', lambda: Path(tmpdir))

        write_config_yaml(target_dir)

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

        assert conf.load_config() == {
            'bells': {'big_ben': {'dongs': 12}},
            'whistles': ['referee', 'dog'],
        }

    def test_raises_ConfigFormatError_with_bad_yaml_input(self, test_config):
        with pytest.raises(ConfigFormatError):
            test_config(yaml_input="""
            - un
            - deux
            - trois
            """)

    def test_passing_mapping_to_update_adds_keys_as_attrs_with_values(self, test_config):
        """Test the update method adds key value pairs as new class attributes."""
        conf = test_config()
        conf.update({
            'beaches': ['porthcurno', 'sennen'],
            'roads': 'A30',
        })
        assert conf.beaches == ['porthcurno', 'sennen']
        assert conf.roads == 'A30'

    def test_set_attrs_just_updates_if_to_unpack_not_given(self, test_config):
        """Test set_attrs method behaves like update method when to_unpack not given."""
        conf1 = test_config()
        # Loads the same config file as test_config does.
        conf2 = Config('my_config')

        attrs = {'colour': 'green', 'shape': 'circle'}
        conf1.set_attrs(attrs)
        conf2.update(attrs)

        assert vars(conf1) == vars(conf2)

    def test_set_attrs_unpacks_given_mapping_attrs_directly(self, test_config):
        """Test set_attrs method unpacks mappings at attrs given by
        to_unpack directly as attributes.
        """
        conf = test_config()
        attrs = {
            'names': {'jenny': 'female', 'bruce': 'male'},
            'kids': {'terrence': {'age': 12}},
        }
        conf.set_attrs(attrs, to_unpack=['names'])

        attr_keys = vars(conf).keys()
        assert 'jenny' in attr_keys
        assert 'bruce' in attr_keys
        # The mapping at 'names' is also in attribute, in addition to
        # the mapping being unpacked directly.
        assert 'names' in attr_keys
        assert 'kids' in attr_keys

    def test_set_attrs_raises_TypeError_when_attr_to_unpack_is_not_mapping(
        self, test_config
    ):
        """Tests raises TypeError because 'rice' attr is not a mapping"""
        conf = test_config()
        attrs = {'rice': ['uncle_bens', 'tilda'], 'pasta': ['napolina']}
        with pytest.raises(TypeError):
            conf.set_attrs(attrs, to_unpack=['rice'])

    @pytest.mark.parametrize('bad_attr', ['yelp', [1, 2, 3], 5.67])
    def test_set_attrs_raises_ConfigFormatError_when_not_passed_mapping(
        self, test_config, bad_attr
    ):
        """Tests raises ConfigFormatError when using set_attrs."""
        conf = test_config()
        with pytest.raises(ConfigFormatError):
            conf.set_attrs(bad_attr)

    def test_flatten_nested_dicts_only_flattens_given_attrs(self, test_config):
        """Test flatten_nested_dicts flattens the mappings at the given attrs."""
        conf = test_config(yaml_input="""
        bells:
            big_ben:
                dongs: 12
        appearances:
            batman:
                joker: 27
                deadshot: 7
                killer_croc: 12
        """)
        conf.flatten_nested_dicts(['bells'])
        assert conf.bells == {('big_ben', 'dongs') : 12}
        # Check appearances unchanged.
        assert conf.appearances == {'batman': {'joker': 27, 'deadshot': 7, 'killer_croc': 12}}

    def test_get_key_value_pairs_returns_pairs_for_given_attrs(
        self, test_config, all_in_output
    ):
        """Test returns key value pairs for specified attributes."""
        conf = test_config(yaml_input="""
        paris:
            landmarks:
                - arc de triomphe
                - eiffel tower
            museums:
                - le louvre
        london:
            landmarks:
                - big ben
                - st paul's cathedral
        """)
        conf.get_key_value_pairs(['paris'])
        # Use all_in_output as order of a dict is ambiguous.
        assert all_in_output(
            output=conf.paris,
            values=[
                ('landmarks', 'arc de triomphe'),
                ('landmarks', 'eiffel tower'),
                ('museums', 'le louvre'),
            ],
        )
        # Check London unchanged.
        assert conf.london == {'landmarks': ['big ben', 'st paul\'s cathedral']}

    def test_fill_tuples(self, test_config):
        """Test fill_tuples works in place for config attributes."""
        conf = test_config()
        conf.update({'scanner': ['retailer_1', 'retailer_2', 'retailer_3']})
        conf.fill_tuples(['scanner'], repeat=True, length=2)
        assert conf.scanner == [
            ('retailer_1', 'retailer_1'),
            ('retailer_2', 'retailer_2'),
            ('retailer_3', 'retailer_3'),
        ]

    def test_fill_tuple_keys(self, test_config):
        """Test fill_tuple_keys works in place for config attributes."""
        conf = test_config()
        conf.update({
            'food': {
                ('burger', 'patty', 'meat', 'beef'): 4,
                ('burger', 'patty', 'veggie', 'bean'): 3,
                ('burger', 'sauces', 'spicy', 'piri piri'): 1,
                ('hotdog', 'sausage', 'pork',): 3,
                ('chips'): 2
            }
        })
        conf.fill_tuple_keys(['food'], repeat=True)
        assert conf.food == {
            ('burger', 'patty', 'meat', 'beef'): 4,
            ('burger', 'patty', 'veggie', 'bean'): 3,
            ('burger', 'sauces', 'spicy', 'piri piri'): 1,
            ('hotdog', 'hotdog', 'sausage', 'pork',): 3,
            ('chips', 'chips', 'chips', 'chips'): 2
        }

    @parametrize_cases(
        Case(
            "extend_list_with_list",
            attr_val=['jasmine', 'ivy'],
            extend_vals=['bramble', 'lavender'],
            expected=['jasmine', 'ivy', 'bramble', 'lavender'],
        ),
        Case(
            "extend_list_with_tuple",
            attr_val=['jasmine', 'ivy'],
            extend_vals=('bramble', 'lavender'),
            expected=['jasmine', 'ivy', 'bramble', 'lavender'],
        ),
        Case(
            "extend_list_with_single_value_not_sequence",
            attr_val=['jasmine', 'ivy'],
            extend_vals='bramble',
            expected=['jasmine', 'ivy', 'bramble'],
        ),
        Case(
            "extend_tuple_with_tuple",
            attr_val=('jasmine', 'ivy'),
            extend_vals=('bramble',),
            expected=('jasmine', 'ivy', 'bramble'),
        ),
        Case(
            "extend_tuple_with_list",
            attr_val=('jasmine', 'ivy'),
            extend_vals=['bramble'],
            expected=('jasmine', 'ivy', 'bramble'),
        ),
        Case(
            "extend_tuple_with_single_value_not_sequence",
            attr_val=('jasmine', 'ivy'),
            extend_vals='bramble',
            expected=('jasmine', 'ivy', 'bramble'),
        ),
    )
    def test_extend_attr(
        self, test_config,
        attr_val, extend_vals, expected
    ):
        """Test extend_attrs works for tuple and list attrs."""
        conf = test_config()
        conf.update({'plants': attr_val})
        conf.extend_attr('plants', extend_vals)
        assert conf.plants == expected

    @pytest.mark.parametrize(
        'attr_val', [{'rosemary'}, 'rosemary', 5, {'one': 1}],
        ids=lambda x: f"{type(x)}"
    )
    def test_extend_attrs_raises_when_attr_is_wrong_type(
        self, test_config, attr_val,
    ):
        conf = test_config()
        conf.update({'plants': attr_val})
        with pytest.raises(AttributeError):
            conf.extend_attr('plants', ['heather'])

    def test_prepend_dir(self, test_config, make_path_like):
        """Test that the method prepends the given directory to the attrs."""
        conf = test_config()
        conf.update({'mappers': make_path_like('jobs/places.csv')})

        test_dir = 'people'
        conf.prepend_dir(['mappers'], dir=test_dir)
        assert conf.mappers == 'people/jobs/places.csv'


@pytest.fixture
def mock_dev_config(mocker):
    """Mock up a dev_config class with the mappers_dir attribute."""
    dev_config_mock = mocker.MagicMock()
    dev_config_mock.mappers_dir = 'my_dir'
    return dev_config_mock


@pytest.fixture
def scenario_scan_config(test_config, monkeypatch, mock_dev_config):
    """Return a ScanScenarioConfig with the given yaml input."""
    def _(
        yaml_input: str,
        mock_methods_on_init: Union[bool, Sequence[str]] = False
    ) -> ScanScenarioConfig:
        test_config(yaml_input, name='scenario_config')

        # Monkeypatch each of the methods given by mock_methods_on_init.The
        # methods return None, so just patch with that.
        if mock_methods_on_init is True:
            mock_methods_on_init = ['prepend_dir', 'combine_input_data']
        elif not mock_methods_on_init:
            mock_methods_on_init = []

        with monkeypatch.context() as mp:
            for method in mock_methods_on_init:
                mp.setattr(
                    ScanScenarioConfig,
                    method,
                    lambda *args, **kwargs: None,
                )

            return ScanScenarioConfig(
                'scenario_config',
                dev_config=mock_dev_config,
                subdir=None,
            )
    return _


class TestScanScenarioConfig:
    """Group of tests for ScenarioConfig."""

    @pytest.mark.skip(reason="test shell")
    def test_validate(self):
        """Test for this."""
        pass

    def test_combines_input_data_on_init(
        self, all_in_output, scenario_scan_config,
    ):
        """Test that with supplier and without supplier input data is
        combined on init.
        """
        test_yaml = """
        input_data:
            without_supplier:
                - retailer_1
                - retailer_2
            with_supplier:
                supplier_3:
                    retailer_3
        """
        conf = scenario_scan_config(
            test_yaml,
            mock_methods_on_init=['prepend_dir'],
        )
        assert all_in_output(
            output=conf.input_data,
            values=[
                ('supplier_3', 'retailer_3'),
                ('retailer_1', 'retailer_1'),
                ('retailer_2', 'retailer_2'),
            ],
        )

    @parametrize_cases(
        Case(
            label="when_both_with_and_without_supplier",
            yaml_input="""
            input_data:
                without_supplier:
                    - retailer_1
                    - retailer_2
                with_supplier:
                    supplier_3:
                        retailer_3
            """,
            expected=[
                ('supplier_3', 'retailer_3'),
                ('retailer_1', 'retailer_1'),
                ('retailer_2', 'retailer_2'),
            ],
        ),
        Case(
            label="when_only_with_supplier",
            yaml_input="""
            input_data:
                with_supplier:
                    supplier_2:
                        retailer_2
                    supplier_3:
                        retailer_3
            """,
            expected=[
                ('supplier_2', 'retailer_2'),
                ('supplier_3', 'retailer_3'),
            ],
        ),
        Case(
            label="when_only_without_supplier",
            yaml_input="""
            input_data:
                without_supplier:
                    - retailer_1
                    - retailer_2
            """,
            expected=[
                ('retailer_1', 'retailer_1'),
                ('retailer_2', 'retailer_2'),
            ],
        ),
    )
    def test_combine_input_data_works(
        self, all_in_output, yaml_input, expected,
        scenario_scan_config,
    ):
        """Test returns tuple pairs for supplier and retailer, when both
        without_supplier and with_supplier specified.
        """
        # Patches both init methods on creation so the
        # .combine_input_data() method can be called after.
        conf = scenario_scan_config(yaml_input, mock_methods_on_init=True)
        conf.combine_input_data()
        # Use all_in_output because dict makes order ambiguous.
        assert all_in_output(
            output=conf.input_data,
            values=expected,
        )


class TestWebScrapedScenarioConfig:
    """Tests for the web scraped scenario configs."""

    @pytest.fixture
    def scenario_conf(self, test_config, mock_dev_config):
        """Return a Scenario Config with both scanner and web_scraped
        input_data and item_mappers.
        """
        test_config("""
        input_data:
            supplier_1:
                - single_item_1
            supplier_2:
                - multi_item_timber
        consumption_segment_mappers:
            supplier_1:
                single_item_1: /mapper/path/single_item_1.parquet
            supplier_2:
                multi_item_timber: /mapper/path/multi_item_timber.parquet
        """)
        # my_config.yaml created by the call to test_config
        return WebScrapedScenarioConfig(
            'my_config',
            dev_config=mock_dev_config,
            subdir=None,
        )

    def test_init_gets_keys_value_pairs_for_input_data(
        self, all_in_output, scenario_conf
    ):
        """Test gets key value pairs for input data as expected."""
        assert all_in_output(
            output=scenario_conf.input_data,
            values=[
                ('supplier_1', 'single_item_1'),
                ('supplier_2', 'multi_item_timber'),
            ]
        )

    def test_init_flattens_consumption_segment_mappers_dict(
        self, scenario_conf
    ):
        """Test consumption segment mappers nested dict is flat."""
        assert scenario_conf.consumption_segment_mappers == {
            ('supplier_1', 'single_item_1'): '/mapper/path/single_item_1.parquet',
            ('supplier_2', 'multi_item_timber'): '/mapper/path/multi_item_timber.parquet',
        }


@pytest.fixture
def dev_config(test_config):
    """Return DevConfig file with columns to be removed."""
    test_config(yaml_input="""
    strata_cols:
        - col_1
        - col_2
    preprocess_cols:
        - col_3
        - col_4
    data_cols:
        - col_6
        - col_7
    """)

    return DevConfig("my_config")


class TestDevConfig:
    """Group of tests for DevConfig."""

    @parametrize_cases(
        Case(
            label="add_list_of_new_values",
            new_strata=['new_1', 'new_2'],
            exp_strata_cols=['col_1', 'col_2', 'new_1', 'new_2'],
            exp_preprocess_cols=['col_3', 'col_4', 'new_1', 'new_2'],
            exp_data_cols=['col_6', 'col_7', 'new_1', 'new_2'],
        ),
        Case(
            label="add_single_new_value",
            new_strata='new_1',
            exp_strata_cols=['col_1', 'col_2', 'new_1'],
            exp_preprocess_cols=['col_3', 'col_4', 'new_1'],
            exp_data_cols=['col_6', 'col_7', 'new_1'],
        ),
        Case(
            label="doesnt_add_if_already_in_cols",
            new_strata='col_1',
            exp_strata_cols=['col_1', 'col_2'],
            exp_preprocess_cols=['col_3', 'col_4', 'col_1'],
            exp_data_cols=['col_6', 'col_7', 'col_1'],
        ),
    )
    def test_add_strata(
        self,
        dev_config,
        new_strata,
        exp_strata_cols,
        exp_preprocess_cols,
        exp_data_cols,
    ):
        """Test add_strata method in DevConfig."""
        dev_config.add_strata(new_strata)

        assert dev_config.strata_cols == exp_strata_cols
        assert dev_config.preprocess_cols == exp_preprocess_cols
        assert dev_config.data_cols == exp_data_cols

    @parametrize_cases(
        Case(
            "adds_when_it_exists",
            scenario_yaml="""
            extra_strata:
                - sedimentary
                - igneous
            """,
            exp_strata_cols=['col_1', 'col_2', 'sedimentary', 'igneous'],
            exp_preprocess_cols=['col_3', 'col_4', 'sedimentary', 'igneous'],
            exp_data_cols=['col_6', 'col_7', 'sedimentary', 'igneous'],
        ),
        Case(
            "doesnt_add_when_it_is_None",
            scenario_yaml="""
            extra_strata:
            """,
            exp_strata_cols=['col_1', 'col_2'],
            exp_preprocess_cols=['col_3', 'col_4'],
            exp_data_cols=['col_6', 'col_7'],
        ),
        Case(
            "doesnt_add_when_there_is_no_extra_strata_section",
            scenario_yaml="""
            other_section: blah
            """,
            exp_strata_cols=['col_1', 'col_2'],
            exp_preprocess_cols=['col_3', 'col_4'],
            exp_data_cols=['col_6', 'col_7'],
        ),
    )
    def test_add_extra_strata_if_exists(
        self, scenario_scan_config, dev_config, scenario_yaml,
        exp_strata_cols, exp_preprocess_cols, exp_data_cols,
    ):
        """Test when the add_extra_strata_if_exists() method is called
        with a ScenarioConfig, that it does nothing when extra strata is
        empty or doesn't exist.
        """
        config = scenario_scan_config(scenario_yaml, mock_methods_on_init=True)

        dev_config.add_extra_strata_from_config_if_exists(config)

        assert dev_config.strata_cols == exp_strata_cols
        assert dev_config.preprocess_cols == exp_preprocess_cols
        assert dev_config.data_cols == exp_data_cols

    @pytest.mark.skip(reason='wrongly implemented in code for discount_col so commented out discount col for now.')
    def test_add_extra_data_cols_from_config(
        self, dev_config, scenario_scan_config,
    ):
        """Test it adds preprocess columns to data_cols attribute."""
        scenario_yaml = """
        preprocessing:
            sales_value_col: sales_value
            discount_col: discount_col
        """
        config = scenario_scan_config(scenario_yaml, mock_methods_on_init=True)

        dev_config.add_extra_data_cols_from_config(config)
        assert dev_config.data_cols == ['col_6', 'col_7', 'sales_value', 'discount_col']


class TestScanDevConfig:
    """Tests for the ScanDevConfig."""

    @pytest.fixture
    def scan_dev_config(self, dev_config, scenario_scan_config):
        """Create an instance of ScanDevConfig with extra strata and
        preprocessing sections.
        """
        yaml_input = """
        extra_strata:
            - sedimentary
            - igneous
        preprocessing:
            sales_value_col: sales_value
            discount_col: discount_col
        """
        config = scenario_scan_config(yaml_input, mock_methods_on_init=True)

        # my_config.yaml is created by the dev_config fixture.
        return ScanDevConfig("my_config", subdir=None, config=config)

    @pytest.mark.skip(reason='wrongly implemented in code for discount_col so commented out discount col for now.')
    def test_adds_extra_strata_to_cols_attrs_on_init(self, scan_dev_config):
        assert all([
            new_col in getattr(scan_dev_config, attr)
            for attr in ['strata_cols', 'preprocess_cols', 'data_cols']
            for new_col in ['sedimentary', 'igneous']
        ])

    @pytest.mark.skip(reason='wrongly implemented in code for discount_col so commented out discount col for now.')
    def test_adds_config_cols_to_data_cols_on_init(
        self, scan_dev_config
    ):
        assert all([
            new_col in getattr(scan_dev_config, 'data_cols')
            for new_col in ['sales_value', 'discount_col']
        ])


@pytest.fixture
def scenario_web_scraped_config(test_config, monkeypatch, mock_dev_config):
    """Return a WebScrapedScenarioConfig with the given yaml input."""
    def _(
        yaml_input: str,
        mock_methods_on_init: Union[bool, Sequence[str]] = False
    ) -> WebScrapedScenarioConfig:
        test_config(yaml_input, name='scenario_config')

        # Monkeypatch each of the methods given by mock_methods_on_init.
        # The methods return None, so just patch with that.
        if mock_methods_on_init is True:
            mock_methods_on_init = [
                'prepend_dir',
                'flatten_nested_dicts',
                'get_key_value_pairs',
            ]
        elif not mock_methods_on_init:
            mock_methods_on_init = []

        with monkeypatch.context() as mp:
            for method in mock_methods_on_init:
                mp.setattr(
                    WebScrapedScenarioConfig,
                    method,
                    lambda *args, **kwargs: None,
                )

            return WebScrapedScenarioConfig(
                'scenario_config',
                dev_config=mock_dev_config,
                subdir=None,
            )
    return _


class WebScrapedDevConfig:
    """Tests for the WebScrapedDevConfig"""

    @pytest.fixture
    def web_dev_config(self, dev_config, scenario_web_scraped_config):
        """Create an instance of ScanDevConfig with extra strata and
        preprocessing sections.
        """
        yaml_input = """
        extra_strata:
            - sedimentary
            - igneous
        """
        config = scenario_web_scraped_config(yaml_input, mock_methods_on_init=True)
        # my_config.yaml is created by the dev_config fixture.
        return WebScrapedDevConfig("my_config", subdir=None, config=config)

    def test_adds_extra_strata_to_cols_attrs_on_init(self, web_dev_config):
        assert all([
            new_col in getattr(web_dev_config, attr)
            for attr in ['strata_cols', 'preprocess_cols', 'data_cols']
            for new_col in ['sedimentary', 'igneous']
        ])


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
