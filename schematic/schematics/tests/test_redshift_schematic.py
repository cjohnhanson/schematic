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

VALUES_TO_TEST = dict(
    too_big_for_int8_positive_integer=9223372036854775808,
    too_big_for_int8_negative_integer=-9223372036854775808,
    too_big_for_int4_positive_integer=2147483648,
    too_big_for_int4_negative_integer=-2147483648,
    too_big_for_int2_positive_integer=32768,
    too_big_for_int2_negative_integer=-32768,
    too_big_for_varchar_or_char_256_single_byte="".join(
        ["x"] * 257),
    too_big_for_varchar_256_multibyte="".join(
        ["字"] * 150),
    too_big_for_varchar_max_single_byte="".join(
        ["x"] * 100000),
    too_big_for_varchar_max_multibyte="".join(
        ["字"] * 25000),
    single_multibyte_character="字",
    single_single_byte_character="a")


class TestRedshiftTableColumnMethods(unittest.TestCase):
    """Test all methods for the RedshiftTableColumn class"""
    pass


class TestRedshiftTableDefinitionMethods(unittest.TestCase):
    """Test all the methods for the RedshiftTableDefinition class"""

    def test_can_instantiate_redshift_table_column(self):
        try:
            redshift_schematic.RedshiftTableColumn(
                name='test',
                column_type=redshift_schematic.RedshiftVarcharType('MAX'),
                distkey=True,
                sortkey=1,
                encoding='LZO',
                notnull=False)
        except Exception as e:
            self.fail(
                "RedshiftTableColumn instantiation failed with error {}".format(e))


class TestRedshiftVarcharTypeMethods(unittest.TestCase):
    """Test all the methods for the RedshiftVarcharType class"""

    def setUp(self):
        self.varchar_256_type = redshift_schematic.RedshiftVarcharType(256)
        self.varchar_max_type = redshift_schematic.RedshiftVarcharType("MAX")

    def test_to_sql_returns_correct_string(self):
        self.assertEqual("VARCHAR (256)",
                         redshift_schematic.RedshiftVarcharType(256).to_sql())
        self.assertEqual("VARCHAR (2)",
                         redshift_schematic.RedshiftVarcharType(2).to_sql())

    def test_instantiation_with_too_large_max_len_raises_valueerror(self):
        with self.assertRaises(ValueError):
            redshift_schematic.RedshiftVarcharType(65536)

    def test_is_value_compatible_with_instance_returns_true_when_not_too_long_single_byte_characters(
            self):
        self.assertTrue(
            self.varchar_256_type.is_value_compatible_with_instance(
                VALUES_TO_TEST['single_single_byte_character']))
        self.assertTrue(
            self.varchar_max_type.is_value_compatible_with_instance(
                VALUES_TO_TEST['single_single_byte_character']))

    def test_is_value_compatible_with_instance_returns_true_when_not_too_long_multibyte_characters(
            self):
        self.assertTrue(
            self.varchar_256_type.is_value_compatible_with_instance(
                VALUES_TO_TEST['single_multibyte_character']))
        self.assertTrue(
            self.varchar_max_type.is_value_compatible_with_instance(
                VALUES_TO_TEST['single_multibyte_character']))

    def test_is_value_compatible_with_instance_returns_false_when_too_long_single_byte_characters(
            self):
        self.assertFalse(self.varchar_256_type.is_value_compatible_with_instance(
            VALUES_TO_TEST['too_big_for_varchar_or_char_256_single_byte']))
        self.assertFalse(self.varchar_max_type.is_value_compatible_with_instance(
            VALUES_TO_TEST['too_big_for_varchar_max_single_byte']))

    def test_is_value_compatible_with_instance_returns_false_when_too_long_multibyte_characters(
            self):
        self.assertFalse(
            self.varchar_256_type.is_value_compatible_with_instance(
                VALUES_TO_TEST['too_big_for_varchar_256_multibyte']))
        self.assertFalse(
            self.varchar_max_type.is_value_compatible_with_instance(
                VALUES_TO_TEST['too_big_for_varchar_max_multibyte']))

    def test_is_value_compatible_with_class_returns_true_when_not_too_long_single_byte_characters(
            self):
        self.assertTrue(self.varchar_256_type.is_value_compatible_with_class(
            VALUES_TO_TEST['too_big_for_varchar_or_char_256_single_byte']))

    def test_is_value_compatible_with_class_returns_true_when_not_too_long_multibyte_characters(
            self):
        self.assertTrue(self.varchar_256_type.is_value_compatible_with_class(
            VALUES_TO_TEST['too_big_for_varchar_256_multibyte']))

    def test_is_value_compatible_with_class_returns_false_when_too_long_single_byte_characters(
            self):
        self.assertFalse(self.varchar_max_type.is_value_compatible_with_class(
            VALUES_TO_TEST['too_big_for_varchar_max_single_byte']))

    def test_is_value_compatible_with_class_returns_false_when_too_long_multibyte_characters(
            self):
        self.assertFalse(self.varchar_max_type.is_value_compatible_with_class(
            VALUES_TO_TEST['too_big_for_varchar_max_multibyte']))


class TestRedshiftSchematic(unittest.TestCase):
    """Test all the methods for the redshift Schematic class"""
    pass
