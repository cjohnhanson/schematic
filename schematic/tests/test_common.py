# -*- coding: utf-8 -*-
# MIT License
#
# Copyright (c) 2019 Cody J. Hanson
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import schematic
import unittest


class MockSqlMixinClass(schematic.NameSqlMixin):
    name = "MockSqlMixinClass"


class TestNameSqlMixinMethods(unittest.TestCase):

    def test_to_sql(self):
        self.assertEqual(MockSqlMixinClass().to_sql(), "MockSqlMixinClass")


class MockDictableClass(schematic.DictableMixin, object):
    def __init__(self, a, b):
        self.a = a
        self.b = b


class TestDictableMixinMethods(unittest.TestCase):

    def test_to_dict(self):
        self.assertEqual({'a': 1, 'b': 2},
                         MockDictableClass(1, 2).to_dict())

    def test_from_dict(self):
        test_mock_dictable_class = MockDictableClass(1, 2)
        self.assertEqual(test_mock_dictable_class,
                         MockDictableClass.from_dict({'a': 1, 'b': 2}))

    def test_eq_returns_false_when_other_not_dictable(self):
        self.assertFalse(MockDictableClass(1, 2) == 1)
