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
import csv
from os import path


class CSVColumnType(schematic.TableColumnType):
    """The only column type for CSVs. Always just a string."""
    name = "CSVColumnType"
    parameterized = False

    def is_value_compatible_with_class(self, value):
        """Checks to see if the given value can be inserted into a column of
           the group of types described by this class.

        Args:
          value: The value to check, a string
        Returns:
          True
        """
        return True


class CSVTableColumn(schematic.TableColumn):
    """A column for a CSV table.

       For CSVs without a header, columns are given a default name based
       on the type of the column.
    Attributes:
      name: The name of the column.
      column_type: Always CSVColumnType
    """

    def __init__(self, name):
        super(CSVTableColumn, self).__init__(name,
                                             column_type=CSVColumnType())


class CSVTableDefinition(schematic.TableDefinition):
    """CSV-specific implementation of TableDefinition

    Attributes:
      name: the file's basename
      columns: list of CSVTableColumns in this table
    """

    def __init__(self, name, columns=[], handler=None):
        super(CSVTableDefinition, self).__init__(name, columns)
        self.handler = handler

    def get_rows(self):
        """Get rows from this file
        Yields:
          A list of values
        """
        self.handler.seek(0)
        reader = csv.reader(self.handler)
        next(reader)
        for line in reader:
            print(line)
            yield(tuple(line))

    @classmethod
    def from_csv(cls, csv_file):
        """Instantiate a CSVTableDefinition from a csv file
        Args:
          csv_file: an IO object with the CSV data
        """
        csv_file.seek(0)
        header = csv_file.readline()
        columns = []
        for column_name in header.split(","):
            columns.append(CSVTableColumn(column_name))
        csv_file.seek(0)
        return cls(name=path.basename(path.splitext(csv_file.name)[0]),
                   columns=columns,
                   handler=csv_file)


class CSVSchematic(schematic.Schematic):
    """"Schematic implementation for working with CSVs.
    """
    name = 'csv'
    most_restrictive_types = [CSVColumnType]
    table_definition_class = CSVTableDefinition
