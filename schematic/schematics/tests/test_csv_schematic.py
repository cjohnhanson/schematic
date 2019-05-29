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
from schematic.schematics import csv_schematic
import io
import unittest

TEST_CSV_FILE = io.StringIO("""a, b, c, d, e, f, g
1, 2, 3, 4, 5, 6, 7
1, 4, 6, 2, 5, 7, j,
asd, jfkf, aaddd, 1.394, 1.adf, a, b
1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7
2019-01-22T08:00:12, 2019-01-22T08:00:12, 2019-01-22T08:00:12, 2019-01-22T08:00:12, 2019-01-22T08:00:12, 2019-01-22T08:00:12, 2019-01-22T08:00:12
""")
TEST_CSV_FILE.name = 'testing.csv'


class TestCSVColumnTypeMethods(unittest.TestCase):

    def test_is_value_compatible_with_class(self):
        self.assertTrue(csv_schematic.CSVColumnType(
        ).is_value_compatible_with_class("anything"))


class TestCSVTableColumnMethods(unittest.TestCase):

    def test_can_instantiate(self):
        csv_schematic.CSVTableColumn("a name")


class TestCSVTableDefinitionMethods(unittest.TestCase):

    def test_from_csv(self):
        mock_csv_table_def = csv_schematic.CSVTableDefinition("testing")
        for column in TEST_CSV_FILE.readline().split(','):
            mock_csv_table_def.add_column(csv_schematic.CSVTableColumn(column))
        TEST_CSV_FILE.seek(0)
        self.assertEqual(
            mock_csv_table_def,
            csv_schematic.CSVTableDefinition.from_csv(TEST_CSV_FILE))

    def test_get_rows_yields_rows(self):
        TEST_CSV_FILE.readline()
        rows1 = [tuple(l.strip().split(","))
                 for l in TEST_CSV_FILE.readlines()]
        TEST_CSV_FILE.seek(0)
        csv_table_def = csv_schematic.CSVTableDefinition.from_csv(
            TEST_CSV_FILE)
        rows2 = [row for row in csv_table_def.get_rows()]
        self.assertEqual(rows1, rows2)


class TestCSVSchematicMethods(unittest.TestCase):

    def test_can_instantiate(self):
        csv_schematic.CSVSchematic()
