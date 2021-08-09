"""Tests for the _utils module in index_methods."""
import pytest

from cprices.alternative_sources.index_methods._utils import *


class TestListConvert:

    def test_leaves_list_as_is(self):
        assert list_convert(['beans', 'toast']) == ['beans', 'toast']

    def test_converts_tuple_to_list(self):
        assert list_convert(('carnage', 'venom')) == ['carnage', 'venom']

    def test_wraps_string_in_list_container(self):
        assert list_convert('rice') == ['rice']

    @pytest.mark.parametrize('obj', [67, 2.75, {'eggs', 'sausage'}])
    def test_wraps_other_objs_in_list_container(self, obj):
        assert list_convert(obj) == [obj]

    def test_returns_None_if_None_passed(self):
        assert list_convert(None) is None


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
