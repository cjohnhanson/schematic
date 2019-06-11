# -*- coding: utf-8 -*-
# MIT License
#
# Copyright (c) 2019 Cody J. Hanson

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
import re
from psycopg2 import sql
from schematic.schematics.redshift_schematic import *

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

    def test_can_instantiate_redshift_table_column(self):
        RedshiftTableColumn(
            name='test',
            column_type=RedshiftVarcharType(256),
            distkey=True,
            sortkey=1,
            encoding='LZO',
            notnull=False)


ROWS = [("varchar_defaults",
         "character varying(256)",
         "none",
         False,
         0,
         False),
        ("varchar_no_defaults",
         "character varying(256)",
         "lzo",
         True,
         1,
         True),
        ("boolean_defaults",
         "boolean",
         "none",
         False,
         0,
         False),
        ("numeric_1_1_defaults",
         "numeric(1,1)",
         "none",
         False,
         0,
         False)
        ]


class MockCursorObject():
    """A mock cursor object that just returns the given rows"""

    def __init__(self):
        self.rows = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def execute(self, sql):
        self.executed_with = sql
        self.rows = ROWS

    def fetchall(self):
        for row in self.rows:
            yield row


class MockConnObject():
    def __init__(self):
        self.cursor = MockCursorObject


class TestRedshiftTableDefinitionMethods(unittest.TestCase):
    """Test all the methods for the RedshiftTableDefinition class"""

    def setUp(self):
        # TODO: set up all the mock columns and multiple table definitions
        mock_varchar_256_type = RedshiftVarcharType(256)
        mock_varchar_1_type = RedshiftVarcharType(1)
        mock_boolean_type = RedshiftBooleanType()
        mock_numeric_1_1_type = RedshiftDecimalType((1, 1))
        self.mock_columns_dict = {
            "varchar_defaults": RedshiftTableColumn("varchar_defaults",
                                                    mock_varchar_256_type),
            "varchar_no_defaults": RedshiftTableColumn("varchar_no_defaults",
                                                       mock_varchar_256_type,
                                                       encoding="LZO",
                                                       distkey=True,
                                                       sortkey=1,
                                                       notnull=True,
                                                       primary_key=True),
            "boolean_defaults": RedshiftTableColumn("boolean_defaults",
                                                    mock_boolean_type),
            "numeric_1_1_defaults": RedshiftTableColumn("numeric_1_1_defaults",
                                                        mock_numeric_1_1_type)
        }
        self.mock_table_no_columns = RedshiftTableDefinition("mock",
                                                             "no_columns",
                                                             [])
        self.mock_table_all_columns = RedshiftTableDefinition("mock",
                                                              "all_columns",
                                                              [])
        for k, v in self.mock_columns_dict.items():
            self.mock_table_all_columns.add_column(v)
        self.mock_table_single_varchar_column = RedshiftTableColumn(
            "mock", "single_varchar_column", self.mock_columns_dict['varchar_no_defaults'])
        self.conn = MockConnObject()

    def test_can_instantiate_redshift_table_definition(self):
        RedshiftTableDefinition(
            schema="test_schema",
            name="test_name",
            columns=[RedshiftTableColumn("test_column1",
                                         RedshiftVarcharType(256),
                                         distkey=False,
                                         sortkey=2,
                                         encoding="LZO",
                                         notnull=False),
                     RedshiftTableColumn("test_column2",
                                         RedshiftDateType(),
                                         distkey=True)])

    def test_can_instantiate_from_source(self):
        table_def = RedshiftTableDefinition.from_source(
            self.conn, "mock", "all_columns")
        self.assertEqual(table_def, self.mock_table_all_columns)

    # def test_column_create_sql_no_encoding(self):
    #     self.fail("TODO")

    # def test_column_create_sql_with_encoding(self):
    #     self.fail("TODO")

    # def test_create_sql_no_distkey_or_sortkey(self):
    #     self.fail("TODO")

    # def test_create_sql_sortkey_and_distkey(self):
    #     self.fail("TODO")

    # def test_create_table_successfully_creates(self):
    #     self.fail("TODO")

    # def test_create_table_raises_programming_error_if_exists(self):
    #     self.fail("TODO")

    # def test_get_rows_yields_rows(self):
    #     self.fail("TODO")


class TestRedshiftVarcharTypeMethods(unittest.TestCase):
    """Test all the methods for the RedshiftVarcharType class"""

    def setUp(self):
        self.varchar_256_type = RedshiftVarcharType(256)
        self.varchar_max_type = RedshiftVarcharType(
            RedshiftSchematic().MAX_VARCHAR_BYTES)

    def test_to_sql_returns_correct_string(self):
        self.assertEqual(sql.SQL("VARCHAR (256)"),
                         RedshiftVarcharType(256).to_sql())
        self.assertEqual(sql.SQL("VARCHAR (2)"),
                         RedshiftVarcharType(2).to_sql())

    def test_instantiation_with_too_large_max_len_raises_valueerror(self):
        with self.assertRaises(ValueError):
            RedshiftVarcharType(65536)

    def test_value_is_compatible_returns_true_when_not_too_long_single_byte_characters(
            self):
        self.assertTrue(
            self.varchar_256_type.value_is_compatible(
                VALUES_TO_TEST['single_single_byte_character']))
        self.assertTrue(
            self.varchar_max_type.value_is_compatible(
                VALUES_TO_TEST['single_single_byte_character']))

    def test_value_is_compatible_returns_true_when_not_too_long_multibyte_characters(
            self):
        self.assertTrue(
            self.varchar_256_type.value_is_compatible(
                VALUES_TO_TEST['single_multibyte_character']))
        self.assertTrue(
            self.varchar_max_type.value_is_compatible(
                VALUES_TO_TEST['single_multibyte_character']))

    def test_value_is_compatible_returns_false_when_too_long_single_byte_characters(
            self):
        self.assertFalse(self.varchar_256_type.value_is_compatible(
            VALUES_TO_TEST['too_big_for_varchar_or_char_256_single_byte']))
        self.assertFalse(self.varchar_max_type.value_is_compatible(
            VALUES_TO_TEST['too_big_for_varchar_max_single_byte']))

    def test_value_is_compatible_returns_false_when_too_long_multibyte_characters(
            self):
        self.assertFalse(
            self.varchar_256_type.value_is_compatible(
                VALUES_TO_TEST['too_big_for_varchar_256_multibyte']))
        self.assertFalse(
            self.varchar_max_type.value_is_compatible(
                VALUES_TO_TEST['too_big_for_varchar_max_multibyte']))

    def test__value_is_compatible_superset_returns_true_when_not_too_long_single_byte_characters(
            self):
        self.assertTrue(self.varchar_256_type._value_is_compatible_superset(
            VALUES_TO_TEST['too_big_for_varchar_or_char_256_single_byte']))

    def test__value_is_compatible_superset_returns_true_when_not_too_long_multibyte_characters(
            self):
        self.assertTrue(self.varchar_256_type._value_is_compatible_superset(
            VALUES_TO_TEST['too_big_for_varchar_256_multibyte']))

    def test__value_is_compatible_superset_returns_false_when_too_long_single_byte_characters(
            self):
        self.assertFalse(self.varchar_max_type._value_is_compatible_superset(
            VALUES_TO_TEST['too_big_for_varchar_max_single_byte']))

    def test__value_is_compatible_superset_returns_false_when_too_long_multibyte_characters(
            self):
        self.assertFalse(self.varchar_max_type._value_is_compatible_superset(
            VALUES_TO_TEST['too_big_for_varchar_max_multibyte']))


class TestRedshiftCharTypeMethods(unittest.TestCase):
    """Test all the methods for the RedshiftCharType class"""

    def test__value_is_compatible_superset_returns_false_multibyte(self):
        self.assertFalse(RedshiftCharType(
            5)._value_is_compatible_superset('字abc'))

    def test_value_is_compatible_returns_false_multibyte(self):
        self.assertFalse(RedshiftCharType(
            5).value_is_compatible('字abc'))

    def test_to_sql_returns_correct_string(self):
        self.assertEqual(RedshiftCharType(5).to_sql(),
                         sql.SQL('CHAR (5)'))


class TestRedshiftBooleanTypeMethods(unittest.TestCase):
    """Test all the methods of the RedshiftBooleanType class"""

    def test_to_sql_returns_correct_string(self):
        self.assertEqual(RedshiftBooleanType().to_sql(),
                         sql.SQL("BOOLEAN"))


class TestRedshiftTimestampTypeMethods(unittest.TestCase):
    """Test all the methods of the RedshiftBooleanType class"""

    def test_to_sql_returns_correct_string(self):
        self.assertEqual(RedshiftTimestampType().to_sql(),
                         sql.SQL("TIMESTAMP"))


class TestRedshiftTimestampTZTypeMethods(unittest.TestCase):
    """Test all the methods of the RedshiftBooleanType class"""

    def test_to_sql_returns_correct_string(self):
        self.assertEqual(RedshiftTimestampTZType().to_sql(),
                         sql.SQL("TIMESTAMPTZ"))


class TestRedshiftTimestampDateMethods(unittest.TestCase):
    """Test all the methods of the RedshiftBooleanType class"""

    def test_to_sql_returns_correct_string(self):
        self.assertEqual(RedshiftDateType().to_sql(),
                         sql.SQL("DATE"))


class TestRedshiftDecimalTypeMethods(unittest.TestCase):
    """Test all the methods of the RedshiftDecimalType class"""

    def test_to_sql_returns_correct_string(self):
        self.assertEqual(sql.SQL("DECIMAL(5, 5)"),
                         RedshiftDecimalType((5, 5)).to_sql())

    def test__value_is_compatible_superset_returns_true(self):
        self.assertTrue(
            RedshiftDecimalType()._value_is_compatible_superset("12.304"))

    def test__value_is_compatible_superset_returns_true_no_decimal(self):
        self.assertTrue(
            RedshiftDecimalType()._value_is_compatible_superset("12"))

    def test__value_is_compatible_superset_returns_false_multiple_decimal(
            self):
        self.assertFalse(
            RedshiftDecimalType()._value_is_compatible_superset("12.0.0"))

    def test_get_parameter_no_decimal_returns_precision(self):
        self.assertEqual((5, 0),
                         RedshiftDecimalType.get_parameter_for_value("12345"))


class TestRedshiftDoublePrecisionTypeMethods(unittest.TestCase):

    def test_to_sql_returns_correct_string(self):
        self.assertEqual(sql.SQL("DOUBLE PRECISION"),
                         RedshiftDoublePrecisionType().to_sql())


class TestRedshiftRealTypeMethods(unittest.TestCase):

    def test_to_sql_returns_correct_string(self):
        self.assertEqual(sql.SQL("REAL"),
                         RedshiftRealType().to_sql())


class TestRedshiftBigIntTypeMethods(unittest.TestCase):

    def test_to_sql_returns_correct_string(self):
        self.assertEqual(sql.SQL("BIGINT"),
                         RedshiftBigIntType().to_sql())

    def test__value_is_compatible_superset_returns_false_not_a_number(self):
        self.assertFalse(
            RedshiftBigIntType()._value_is_compatible_superset("Not a number"))


class TestRedshiftIntTypeMethods(unittest.TestCase):

    def test_to_sql_returns_correct_string(self):
        self.assertEqual(sql.SQL("INT"),
                         RedshiftIntType().to_sql())


class TestRedshiftSmallIntType(unittest.TestCase):

    def test_to_sql_returns_correct_string(self):
        self.assertEqual(sql.SQL("SMALLINT"),
                         RedshiftSmallIntType().to_sql())


class TestRedshiftSchematic(unittest.TestCase):
    """Test all the methods for the redshift Schematic class"""

    def test_get_type_returns_none_null_string(self):
        self.assertEqual(None,
                         RedshiftSchematic().get_type("None"))

    def test_get_type_returns_previous_null_string(self):
        self.assertEqual(
            RedshiftSchematic().get_type(
                "",
                previous_type=RedshiftDateType()),
            RedshiftDateType())

    def test_get_type_returns_bigger_varchar_when_previous_type_too_small_varchar(
            self):
        self.assertEqual(
            RedshiftSchematic().get_type(
                "11CHARACTER",
                previous_type=RedshiftVarcharType(10)),
            RedshiftVarcharType(11))

    def test_get_type_returns_previous_when_compatible_varchar(self):
        self.assertEqual(
            RedshiftSchematic().get_type(
                "smaller",
                previous_type=RedshiftVarcharType(256)),
            RedshiftVarcharType(256))

    def test_get_type_returns_varchar_no_previous_type(self):
        self.assertEqual(RedshiftSchematic().get_type("astring"),
                         RedshiftVarcharType(7))

    def test_get_type_returns_varchar_no_previous_type_multibyte(self):
        self.assertEqual(RedshiftSchematic().get_type("字string"),
                         RedshiftVarcharType(9))

    def test_get_type_returns_bool_when_no_previous_type(self):
        self.assertEqual(RedshiftSchematic().get_type("TRUE"),
                         RedshiftBooleanType())

    def test_get_type_returns_bool_when_previous_type_bool(self):
        self.assertEqual(
            RedshiftSchematic().get_type(
                "f",
                previous_type=RedshiftBooleanType()),
            RedshiftBooleanType())

    def test_get_type_returns_small_int_no_previous_type(self):
        self.assertEqual(RedshiftSchematic().get_type("478"),
                         RedshiftBigIntType())

    def test_get_type_returns_small_int_previous_type_small_int(self):
        self.assertEqual(
            RedshiftSchematic().get_type(
                "943",
                previous_type=RedshiftSmallIntType()),
            RedshiftSmallIntType())

    def test_get_type_returns_int_previous_type_small_int(self):
        self.assertEqual(
            RedshiftSchematic().get_type(
                "-33500",
                previous_type=RedshiftSmallIntType()),
            RedshiftIntType())

    def test_get_type_returns_bigint_previous_type_int(self):
        self.assertEqual(
            RedshiftSchematic().get_type(
                "-2147483760",
                previous_type=RedshiftIntType()),
            RedshiftBigIntType())

    def test_get_type_returns_bigint_previous_type_boolean(self):
        self.assertEqual(
            RedshiftSchematic().get_type(
                "12",
                previous_type=RedshiftBooleanType()),
            RedshiftBigIntType())

    def test_get_type_returns_float_previous_type_smallint(self):
        self.assertEqual(
            RedshiftSchematic().get_type(
                "-943.1",
                previous_type=RedshiftSmallIntType()),
            RedshiftRealType())

    def test_get_type_returns_double_previous_type_float(self):
        self.assertEqual(
            RedshiftSchematic().get_type(
                "123456.123456",
                previous_type=RedshiftRealType()),
            RedshiftDoublePrecisionType())

    def test_get_type_returns_decimal_previous_type_double(self):
        self.assertEqual(
            RedshiftSchematic().get_type(
                "12345678910111213.12345678910111213",
                previous_type=RedshiftDoublePrecisionType()),
            RedshiftDecimalType(
                (17,
                 17)))

    def test_get_type_returns_decimal_previous_type_decimal(self):
        self.assertEqual(
            RedshiftSchematic().get_type(
                "1234.12345",
                previous_type=RedshiftDecimalType((2, 3))),
            RedshiftDecimalType((5, 5)))

    def test_get_type_returns_date_no_previous_type(self):
        self.assertEqual(
            RedshiftSchematic().get_type(
                "2019-06-22"),
            RedshiftDateType())
        self.assertEqual(
            RedshiftSchematic().get_type(
                "today"),
            RedshiftDateType())

    def test_get_type_returns_timestamp_previous_type_date(self):
        self.assertEqual(
            RedshiftSchematic().get_type(
                "2019-06-22T15:01:24.943",
                previous_type=RedshiftDateType()),
            RedshiftTimestampType())

    def test_get_type_returns_timestamp_previous_type_timestamptz(self):
        self.assertEqual(
            RedshiftSchematic().get_type(
                "2019-06-22T11:45:12 PM",
                previous_type=RedshiftTimestampTZType()),
            RedshiftTimestampType())

    def test_get_type_from_string_returns_varchar(self):
        self.assertEqual(
            RedshiftSchematic().get_type_from_string("character varying(256)"),
            RedshiftVarcharType(256))


class TestDatePatterns(unittest.TestCase):

    def setUp(self):
        self.valid_time_strings = [
            "T19:14:32.123453",
            " 01:58:32 AM",
            "T12:29 PM"]
        self.invalid_time_strings = [
            "T 12:14:32",
            " a3:34:39",
            "T42:12:39.3924"
            "abc",
            "3.14159"]
        self.valid_date_strings = [
            "2019-03-09",
            "20190622",
            "11/2/19",
            "12/04/2019",
            "20190622"
        ]
        self.invalid_date_strings = [
            "2019-13-12",
            "3129-01-33",
            "122345",
            "abcde"
        ]
        self.valid_timezone_strings = [
            "+08:00"
        ]
        self.invalid_timezone_strings = [
            "CSTa"
        ]

        self.valid_timestamp_pattern = "^({})$".format(
            VALID_DATE_PATTERN + VALID_TIME_PATTERN)

        self.valid_timestamptz_pattern = "^({})$".format(
            VALID_DATE_PATTERN + VALID_TIME_PATTERN + VALID_TIMEZONE_PATTERN)

    def matches(self, pattern, string):
        return bool(re.compile(pattern).match(string))

    def test_valid_matches_times(self):
        pattern = VALID_TIME_PATTERN
        for valid_string in self.valid_time_strings:
            self.assertTrue(self.matches(pattern, valid_string),
                            msg="pattern: {} string: {}".format(pattern,
                                                                valid_string))

    def test_invalid_no_matches_times(self):
        pattern = VALID_TIME_PATTERN
        for invalid_string in self.invalid_time_strings:
            self.assertFalse(
                self.matches(
                    pattern, invalid_string), msg="pattern: {} string: {}".format(
                    pattern, invalid_string))

    def test_valid_matches_dates(self):
        pattern = VALID_DATE_PATTERN
        for valid_string in self.valid_date_strings:
            self.assertTrue(self.matches(VALID_DATE_PATTERN, valid_string),
                            msg="pattern: {} string: {}".format(pattern,
                                                                valid_string))

    def test_invalid_no_matches_dates(self):
        pattern = VALID_DATE_PATTERN
        for invalid_string in self.invalid_date_strings:
            self.assertFalse(
                self.matches(
                    pattern, invalid_string), msg="pattern: {} string: {}".format(
                    pattern, invalid_string))

    def test_valid_matches_timezones(self):
        pattern = VALID_TIMEZONE_PATTERN
        for valid_string in self.valid_timezone_strings:
            self.assertTrue(self.matches(pattern, valid_string),
                            msg="pattern: {} string: {}".format(pattern,
                                                                valid_string))

    def test_invalid_no_matches_timezones(self):
        pattern = VALID_TIMEZONE_PATTERN
        for invalid_string in self.invalid_timezone_strings:
            self.assertFalse(
                self.matches(
                    pattern, invalid_string), msg="pattern: {} string: {}".format(
                    pattern, invalid_string))

    def test_valid_timestamps_match(self):
        pattern = self.valid_timestamp_pattern
        for valid_ds in self.valid_date_strings:
            for valid_ts in self.valid_time_strings:
                valid_string = valid_ds + valid_ts
                self.assertTrue(
                    self.matches(
                        pattern, valid_string), msg="pattern: {} string: {}".format(
                        pattern, valid_string))
