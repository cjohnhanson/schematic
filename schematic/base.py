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
import click
from psycopg2 import sql
from csv import DictReader
from schematic import NameSqlMixin, DictableMixin, NextLessRestrictiveCycleError


class TableColumnType(NameSqlMixin, DictableMixin, object):
    """Represents a type for a table column.

    The restrictivity of two different column types can be compared using
    the built in comparison operators. For two TableColumnTypes, A and B,
    if A is less restrictive than B, then A < B evaluates to True.

    Note that TableColumnTypes are only fully comparable when in the
    same subtree. If A is not found when traversing next_less_restrictive
    in B and vice versa, A > B, A < B, and A == B all evaluate to False.

    Attributes:
      name: The name of the type
      next_less_restrictive: The TableColumnType which is next less restrictive
      name_regex: A Regex matching all strings which are a valid
                  string representation of this type
    """
    name = "TableColumnType"
    next_less_restrictive = None
    name_regex = None

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        if isinstance(
                other,
                TableColumnType) or issubclass(
                type(other),
                TableColumnType):
            return self.name == other.name
        else:
            print("Calling __eq_({}, {})".format(self, other))
            return False

    def __lt__(self, other):
        nlr = other
        while nlr:
            nlr = nlr.next_less_restrictive() if nlr.next_less_restrictive else None
            if nlr == self:
                return True
        return False

    def __gt__(self, other):
        return other < self

    def is_value_compatible_with_instance(self, value):
        """Checks to see if the given value can be inserted into a column of
           the type described by this instance.

        Args:
          value: The value to check, a string
        Raises:
          NotImplementedError: This should be implemented by subclasses.
        """
        raise NotImplementedError

    def is_value_compatible_with_class(self, value):
        """Checks to see if the given value can be inserted into a column of
           the group of types described by this class.

           This is used, for example, to check if a value could
           fit into any VARCHAR class in a SQL database, whereas
           is_value_compatible_with_instance would be used
           to check if a value could fit into specifically
           into a VARCHAR(256) class.

        Args:
          value: The value to check, a string
        Raises:
          NotImplementedError: This should be implemented by subclasses.
        """
        raise NotImplementedError


class TableColumn(DictableMixin, NameSqlMixin, object):
    """DB-agnostic base class for storing info about a column in a table.

    Attributes:
      name: The identifier for the column
      type: A TableColumnType instance with type information for the column.
    """

    def __init__(self, name, type):
        self.name = name
        self.type = type

    def __repr__(self):
        return "{}: {}".format(self.name, self.type)


class TableDefinition(DictableMixin, NameSqlMixin, object):
    """DB-agnostic base class for storing info about a table

    Attributes:
      name: The identifier for this table.
      columns: A list of TableColumns describing this table's columns.
    """

    def __init__(self, name, columns):
        self.name = name
        self.columns = columns

    def create_sql(self):
        """Generate a sql statement for creating a table based on this TableDefinition.

        Raises:
          NotImplementedError: Subclasses should implement this.
        """
        raise NotImplementedError

    def create_table(self, *args, **kwargs):
        """Create the table in the destination
            specified in *args and **kwargs

        Raises:
          NotImplementedError: Subclasses should implement this.
        """
        raise NotImplementedError

    def add_column(self, column):
        """Add a column to the column list.

        Args:
          column: A TableColumn instance to be added to this instance's columns.
        """
        for col in self.columns:
            if col.name == column.name:
                raise ValueError(
                    "Column with name {} already exists in TableDefinition {}".format(
                        column.name, self.name))
        self.columns.append(column)

    def update_column(self, column):
        """Update the existing column with name column.name to the given column.

        Args:
          column: A TableColumn instance to be added to this instance's columns.
        Raises:
          ValueError: If TableColumn with column.name does not exist in columns
        """
        for i, col in enumerate(self.columns):
            if col.name == column.name:
                self.columns[i] = column
                return
        raise ValueError(
            "No such column name {} in {}".format(
                column.name, self.name))


class Schematic(DictableMixin, object):
    """Interface for implementation specifics for a type of database or warehouse.

    The TableColumnTypes in a given schematic form a tree with the most
    restrictive types being leaf nodes and the least restrictive type
    being the root node.

    Attributes:
      name: Static attribute with the name of this schematic
      most_restrictive_types: the leaf nodes of the restrictivity tree
      table_def: implementation of TableDefinition for this schematic
    """
    name = 'schematic'
    most_restrictive_types = []
    table_definition_class = TableDefinition

    def get_type(self, value, previous_type=None):
        """Get what type of column the given value would be.

        Args:
          value: the value to get the type for.
          previous_type: the type that the column this value is in
                         had previously been assigned
        Raises:
          NotImplementedError: This should be implemented by subclasses
        """

        raise NotImplementedError

    def column_types(self):
        """Iterate through column types in this Schematic

        Yields:
          A TableColumnType
        """
        already_yielded = []
        for column_type in self.most_restrictive_types:
            nlr = column_type
            while nlr:
                if nlr.name not in already_yielded:
                    yield nlr.name
                    already_yielded.append(nlr.name)
                nlr = nlr.next_less_restrictive

    def column_type_from_name(self, name):
        """Get the TableColumnTypeInstance described by the given name.

        Args:
          name: The name, e.g. 'VARCHAR(256)'

        Raises:
          NotImplementedError: Subclasses should implement this.
        """
        raise NotImplementedError
