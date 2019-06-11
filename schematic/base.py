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
from abc import ABC
from csv import DictReader
from queue import Queue
from schematic import NameSqlMixin, DictableMixin

class ColumnTypeNotFoundError(Exception):
    pass

class TableColumnType(ABC, NameSqlMixin, DictableMixin):
    """Represents a type for a table column.

    The "size" of two different column types can be compared using
    the built in comparison operators. For two TableColumnTypes, A and B,
    if A is a superset of B, then A > B evaluates to True.

    Note that TableColumnTypes are only fully comparable when in the
    same subtree. If A is not found when traversing next_less_restrictive
    in B and vice versa, A < B, A > B, and A == B will all return False

    Attributes:
      name: The name of the type
      next_less_restrictive: The TableColumnType which is next less restrictive
      parameter: Instance parameter for this class. E.g., '256' for a VARCHAR(256)

    TODO(Cody): Comparisons of incomparable operands should raise an exception
    TODO(Cody): Factor out the "most to least restrictive" iterator logic into a method on this class.
    """
    name = "TableColumnType"
    next_less_restrictive = None
    parameterized = False

    def __init__(self, parameter=None):
        if parameter is not None and not self.parameterized:
            raise ValueError(
                "{} is not a parameterized type.".format(
                    self.name))
        self.parameter = parameter

    def __hash__(self):
        return hash((self.name, self.parameter))

    def __repr__(self):
        if self.parameterized:
            return "{} ({})".format(self.name, self.parameter)
        else:
            return self.name

    def __eq__(self, other):
        return isinstance(
            other, type(self)) and other.parameter == self.parameter

    def __lt__(self, other):
        return other > self

    def __gt__(self, other):
        nlr = other
        while nlr:
            nlr = nlr.next_less_restrictive() if nlr.next_less_restrictive else None
            if nlr == self:
                return True
        return False
        


    def get_depth(self):
        """Get the distance between this TableColumnType
        and the least restrictive TableColumnType in its
        next_less_restrictive linked list

        Returns:
          An int
        """
        depth = -1
        nlr = self
        while nlr:
            nlr = nlr.next_less_restrictive
            depth += 1
        return depth

    def value_is_compatible(self, value):
        """Checks to see if the given value can be inserted into a column of
           the type described by this instance.
        Args:
          value: The value to check, a string
        Returns:
          True if _value_is_compatible_superset(self, value) is True and
          this type isn't parameterized
        Raises:
          NotImplementedError: This should be implemented by subclasses
                               if subclasses are parameterized.
        """
        if self.parameterized:
            raise NotImplementedError
        else:
            return self._value_is_compatible_superset(value)

    def _value_is_compatible_superset(self, value):
        """Checks to see if the given value can be inserted into a column of
           the group of types described by this class.
           This is used, for example, to check if a value could
           fit into any VARCHAR class in a SQL database, whereas
           value_is_compatible would be used
           to check if a value could fit into specifically
           into a VARCHAR(256) class.
        Args:
          value: The value to check, a string
        Raises:
          NotImplementedError: This should be implemented by subclasses.
        """
        raise NotImplementedError

    @staticmethod
    def get_parameter_for_value(value):
        """Get the parameter for a parameterized implementation
           of this class that is required to fit the given value.

        Args:
          value: the value to return the parameter for.
        Raises:
          NotImplementedError: Subclasses should implement this.
        """
        raise NotImplementedError

    @classmethod
    def from_value(cls, value):
        """Create an instance of this class that is compatible with value.

        Args:
          value: The value to return an instance for
        Returns:
          An instance of cls that can fit the value.
        Raises:
          NoCompatibleParameterError: if value cannot fit in any instance of this class.
        """
        if cls.parameterized:
            if not cls()._value_is_compatible_superset(value):
                raise ValueError(
                    "Value {} not compatible with any instance of {}".format(
                        value, cls.name))
            else:
                return cls(parameter=cls.get_parameter_for_value(value))
        else:
            return cls()


class TableColumn(ABC, DictableMixin, NameSqlMixin):
    """DB-agnostic base class for storing info about a column in a table.

    Attributes:
      name: The identifier for the column
      type: A TableColumnType instance with type information for the column.
    """

    def __init__(self, name, column_type):
        self.name = name
        self.column_type = column_type

    def __hash__(self):
        return hash((self.name, self.column_type))

    def __repr__(self):
        return "{}: {}".format(self.name, self.column_type)

    def __eq__(self, other):
        return isinstance(self, type(
            other)) and self.name == other.name and self.column_type == other.column_type


class TableDefinition(ABC, DictableMixin, NameSqlMixin):
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

    def __eq__(self, other):
        return isinstance(
            self,
            type(other)) and self.name == other.name and frozenset(
            self.columns) == frozenset(
            other.columns)

    def __repr__(self):
        return "{}: [{}]".format(self.name, ",".join(
            [str(col) for col in self.columns]))

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
            if col.name.upper() == column.name.upper():
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

    def column_names(self):
        """Get a list of column names for this table."""
        return [col.name for col in self.columns]

    def get_rows(self, *args, **kwargs):
        """Generator for rows of the table described by this TableDefinition.

        Args:
          *args, **kwargs: DB-specific arguments for connection, e.g.,
                           a psycopg2.connection or csv.DictReader object
        Raises:
          NotImplementedError: Subclasses should implement this.
        """
        raise NotImplementedError

    @classmethod
    def from_source(cls, *args, **kwargs):
        """Instantiate from an implementation-specific source (e.g., a CSV file or a DB connection

        Raises:
          NotImplementedError: Subclasses should implement this.
        """
        raise NotImplementedError


class Schematic(ABC, DictableMixin):
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
    column_class = TableColumn
    null_strings = []

    def get_distance_from_leaf_node(self, column_type):
        """Get the distance between the given TableColumnType
        and its nearest leaf node.

        Returns:
          An int
        """
        distances = []
        for ct in self.most_restrictive_types:
            distance = 0
            nlr = ct
            while nlr:
                if nlr == column_type:
                    distances.append(distance)
                    nlr = None
                else:
                    distance += 1
                    nlr = nlr.next_less_restrictive
        if not distances:
            raise ColumnTypeNotFoundError
        return min(distances)

    def get_type(self, value, previous_type=None):
        """Get what type of column the given value would be.

        Args:
          value: the value to get the type for.
          previous_type: the type that the column this value is in
                         had previously been assigned
        Returns:
          TableColumnType that can fit the value and all values
          of previous_type.
        Raises:
          ValueError: if the given value can't fit into a column
                      of any type in this Schematic
        """
        if value in self.null_strings:
            return previous_type
        if not previous_type:
            depth_dict = {}
            for column_type in self.column_types():
                if column_type()._value_is_compatible_superset(value):
                    depth_dict[column_type().get_depth()] = column_type
            if depth_dict:
                return depth_dict[max(depth_dict.keys())].from_value(value)
        elif previous_type.value_is_compatible(value):
            return previous_type
        elif previous_type._value_is_compatible_superset(value):
            return previous_type.from_value(value)
        elif previous_type.next_less_restrictive:
            return self.get_type(
                value, previous_type=previous_type.next_less_restrictive())
        raise ValueError(
            "value {} cannot fit into a column of any type in Schematic {}".format(
                value, self.name))

    def column_types(self):
        """Iterate through column types in this Schematic.

        Yields:
          A TableColumnType
        """
        already_yielded = []
        for column_type in self.most_restrictive_types:
            nlr = column_type
            while nlr:
                if nlr.name not in already_yielded:
                    yield nlr
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

    def table_def_from_rows(self, name, fieldnames, rows, **kwargs):
        """Instantiate a TableDefinition from an iterator of rows.

        Args:
          name: The name of the table to create
          fieldnames: The names of the columns for this table
          rows: An array of arrays, each of which contains values for the fields in fieldnames
          kwargs: implementation-specific keyword arguments to pass as part of instantiation
        """
        column_types = [None] * len(fieldnames)
        for row in rows:
            for idx, value in enumerate(row):
                column_types[idx] = self.get_type(
                    value, previous_type=column_types[idx])
        table_def = self.table_definition_class(
            name=name, columns=[], **kwargs)
        for idx, fieldname in enumerate(fieldnames):
            table_def.add_column(
                self.column_class(
                    fieldname,
                    column_types[idx]))
        return table_def


def _get_subclasses_helper(schematic_class):
    """Get all the subclasses of the given class.

    Args:
      schematic_class: the class to get subclasses for
    Yields:
      A subclass of the given schematic, or the given
      schematic if it has no subclasses.
    """
    for subclass in schematic_class.__subclasses__():
        for subsub in _get_subclasses_helper(subclass):
            yield subsub
    yield schematic_class


def get_all_schematics():
    """Get all the subclasses of Schematic.
    Yields:
      A subclass of Schematic.
    """
    for schematic_class in _get_subclasses_helper(Schematic):
        yield schematic_class


def get_schematic_by_name(name):
    """Get Schematic implementation for the given name

    Args:
      name: The name of the Schematic implementation to return
    Returns:
      A Schematic implementation
    """
    for schematic_class in get_all_schematics():
        if schematic_class.name == name:
            return schematic_class
