#
"""Tests for the helpers module."""
import pytest

from cprices.utils.helpers import *


class TestInvertNestedKeys:
    """Tests for the invert_nested_keys function."""

    def test_inverts_nested_keys_with_depth_2(self):
        d = {
            'julia': {'eyes': 'brown', 'hair': 'blonde'},
            'david': {'eyes': 'green', 'hair': 'black'},
        }
        assert invert_nested_keys(d) == {
            'eyes': {'julia': 'brown', 'david': 'green'},
            'hair': {'julia': 'blonde', 'david': 'black'},
        }

    def test_inverts_nested_keys_with_depth_3(self):
        d = {
            'carey': {'language': {'fluent': 'french', 'learning': ['english', 'swahili']}},
            'jean': {'language': {'fluent': 'english', 'learning': None}},
        }
        assert invert_nested_keys(d) == {
            'fluent': {'language': {'carey': 'french', 'jean': 'english'}},
            'learning': {'language': {'carey': ['english', 'swahili'], 'jean': None}},
        }


class TestGetKeyValuePairs:
    """The fixture all_in_output is used as dict order is not preserved."""

    def test_gets_pairs_when_values_are_lists(self, all_in_output):
        d = {
            'sugary': ['chocolate', 'popcorn', 'jelly babies'],
            'savoury': ['pasties', 'pies'],
        }
        assert all_in_output(
            output=get_key_value_pairs(d),
            values=[
                ('savoury', 'pasties'),
                ('savoury', 'pies'),
                ('sugary', 'chocolate'),
                ('sugary', 'popcorn'),
                ('sugary', 'jelly babies')
            ],
        )

    def test_gets_pairs_when_values_are_tuples(self, all_in_output):
        d = {
            'sugary': ('chocolate', 'popcorn', 'jelly babies'),
            'savoury': ('pasties', 'pies'),
        }
        assert all_in_output(
            output=get_key_value_pairs(d),
            values=[
                ('savoury', 'pasties'),
                ('savoury', 'pies'),
                ('sugary', 'chocolate'),
                ('sugary', 'popcorn'),
                ('sugary', 'jelly babies')
            ],
        )

    def test_gets_pairs_when_values_are_not_tuples_or_lists(self, all_in_output):
        d = {
            'juicy': 'orange',
            'savoury': ('pasties', 'pies'),
            'cans': 5,
        }
        assert all_in_output(
            output=get_key_value_pairs(d),
            values=[
                ('cans', 5),
                ('savoury', 'pasties'),
                ('savoury', 'pies'),
                ('juicy', 'orange'),
            ],
        )


class TestFillTuples:

    @pytest.fixture
    def input_tups(self):
        return [('a', 'b'), ('x', 'y', 'z'), ('m')]

    def test_default_repeat_is_false_backfills_none(self, input_tups):
        assert fill_tuples(input_tups) == [(None, 'a', 'b'), ('x', 'y', 'z'), (None, None, 'm')]

    def test_repeat_is_true_backfills_first_value(self, input_tups):
        expected = fill_tuples(input_tups, repeat=True)
        assert expected == [('a', 'a', 'b'), ('x', 'y', 'z'), ('m', 'm', 'm')]

    def test_repeat_is_true_with_fill_method_ffill_forward_fills_first_value(self, input_tups):
        expected = fill_tuples(input_tups, repeat=True, fill_method='ffill')
        assert expected == [('a', 'b', 'b'), ('x', 'y', 'z'), ('m', 'm', 'm')]

    def test_converts_non_tuple_values_before_filling(self):
        various_objs = ['m', 1, ['n'], ('o', 'p')]
        assert fill_tuples(various_objs) == [(None, 'm'), (None, 1), (None, 'n'), ('o', 'p')]

    def test_returns_input_if_there_are_no_non_string_sequences(self):
        various_objs = ['m', 1, 'n']
        assert fill_tuples(various_objs) == various_objs

    def test_fills_objs_that_are_all_non_string_sequences_to_length_when_given(self):
        various_objs = ['m', 1, 'n']
        assert fill_tuples(various_objs, length=2) == [(None, 'm'), (None, 1), (None, 'n')]


class TestTupleConvert:

    def test_leaves_tuple_as_is(self):
        assert tuple_convert(('beans', 'toast')) == ('beans', 'toast')

    def test_converts_list_to_tuple(self):
        assert tuple_convert(['carnage', 'venom']) == ('carnage', 'venom')

    def test_wraps_string_in_tuple_container(self):
        assert tuple_convert('rice') == ('rice',)

    @pytest.mark.parametrize('obj', [None, 67, 2.75])
    def test_wraps_other_objs_in_tuple_container(self, obj):
        assert tuple_convert(obj) == (obj,)


class TestListConvert:

    def test_leaves_list_as_is(self):
        assert list_convert(['beans', 'toast']) == ['beans', 'toast']

    def test_converts_tuple_to_list(self):
        assert list_convert(('carnage', 'venom')) == ['carnage', 'venom']

    def test_wraps_string_in_list_container(self):
        assert list_convert('rice') == ['rice']

    @pytest.mark.parametrize('obj', [None, 67, 2.75])
    def test_wraps_other_objs_in_list_container(self, obj):
        assert list_convert(obj) == [obj]
