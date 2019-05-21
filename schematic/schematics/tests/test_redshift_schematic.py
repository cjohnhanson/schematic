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
import unittest
from schematic.schematics import redshift_schematic

VALUES_TO_TEST = dict(too_big_for_int8_positive_integer=9223372036854775808,
                      too_big_for_int8_negative_integer=-9223372036854775808,
                      too_big_for_int4_positive_integer=2147483648,
                      too_big_for_int4_negative_integer=-2147483648,
                      too_big_for_int2_positive_integer=32768,
                      too_big_for_int2_negative_integer=-32768,
                      too_big_for_varchar_256_single_byte="TODO",
                      too_big_for_varchar_256_multi_byte="TODO",
                      too_big_for_varchar_max_single_byte="TODO",
                      too_big_for_varchar_max_multi_byte="TODO",
                      too_big_for_char_256_single_byte="TODO",
                      too_big_for_char_256_multi_byte="TODO",)
    
class TestRedshiftTableColumnMethods(unittest.TestCase):
    """Test all methods for the RedshiftTableColumn class"""
    pass

class TestRedshiftTableDefinitionMethods(unittest.TestCase):
    """Test all the methods for the RedshiftTableDefinition class"""
    pass

class TestRedshiftVarcharTypeMethods(unittest.TestCase):
    """Test all the methods for the RedshiftVarcharType class"""

    def test_to_sql_returns_correct_string(self):
        self.assertEqual("VARCHAR (256)",
                         redshift_schematic.RedshiftVarcharType(256).to_sql())
        self.assertEqual("VARCHAR (2)",
                         redshift_schematic.RedshiftVarcharType(2).to_sql())

    def test_instantiation_with_too_large_max_len_raises_valueerror(self):
        self.fail("TODO")

    def test_is_value_compatible_with_instance_returns_true_when_not_too_long_single_byte_characters(self):
        self.fail("TODO")

    def test_is_value_compatible_with_instance_returns_true_when_not_too_long_multi_byte_characters(self):
        self.fail("TODO")

    def test_is_value_compatible_with_instance_returns_false_when_too_long_single_byte_characters(self):
        self.fail("TODO")

    def test_is_value_compatible_with_instance_returns_false_when_too_long_multi_byte_characters(self):
        self.fail("TODO")

    def test_is_value_compatible_with_class_returns_true_when_not_too_long_single_byte_characters(self):
        self.fail("TODO")

    def test_is_value_compatible_with_class_returns_true_when_not_too_long_multi_byte_characters(self):
        self.fail("TODO")

    def test_is_value_compatible_with_class_returns_false_when_too_long_single_byte_characters(self):
        self.fail("TODO")

    def test_is_value_compatible_with_class_returns_false_when_too_long_multi_byte_characters(self):
        self.fail("TODO")

class TestRedshiftSchematic(unittest.TestCase):
    """Test all the methods for the redshift Schematic class"""

    def test_get_type_not_implemented(self):
        self.fail("TODO")

    def test_column_types_yields_all(self):
        self.fail("TODO")
