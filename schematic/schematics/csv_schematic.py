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
          True if the value can be represented by a string, else False.
        """
        try:
            str(value)
            return True
        except:
            return False

class CSVTableColumn(schematic.TableColumn):
    """A column for a CSV table.
    
       For CSVs without a header, columns are given a default name based
       on the type of the column.
    Attributes:
      name: The name of the column. 
      column_type: Always CSVColumnType
    """

    def __init__(self, name, column_type):
        super(CSVTableColumn, self).__init__(name,
                                             column_type=CSVColumnType())

class CSVTableDefinition(schematic.TableDefinition):
    """CSV-specific implementation of TableDefinition

    Attributes:
      name: the file's basename
      columns: list of CSVTableColumns in this table
      filepath: the path to the CSV file described by this definition
    """

    def __init__(self, name, columns, filepath):
        super(CSVTableDefinition, self).__init__(name, columns)
        self.filepath = filepath

    def get_rows(self):
        """Get rows from this file
        Yields:
          A list of values 
        """
        raise NotImplementedError("TODO")


class CSVSchematic(schematic.Schematic):
    """"Schematic implementation for working with CSVs.
    
    """
    name = 'csv'
    most_restrictive_types = [CSVColumnType]
    table_definition_class = CSVTableDefinition

    
            
