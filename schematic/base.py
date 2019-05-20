#!/bin/env python
import click
from psycopg2 import sql
from csv import DictReader
from schematic import NameSqlMixin, DictableMixin, NextLessRestrictiveCycleError

class TableColumnType(NameSqlMixin, DictableMixin, object):
    """Represents a type for a table column.

    The restrictivity of two different column types can be compared using
    the built in comparison operators. For two TableColumnTypes, A and B,
    if A is less restrictive than B, then A < B evaluates to True.

    Attributes:
      name: The name of the type
      next_less_restrictive: The TableColumnType which is next less restrictive
      name_regex: A Regex matching all strings which are a valid 
                  string representation of this type
    """
    name = "TableColumnType"
    next_less_restrictive = None
    name_regex = None
    
    def __init__(self):
        #Make sure this type doesn't create a cycle in next_less_restrictive graph
        nlr = self
        while nlr:
            nlr = nlr.next_less_restrictive
            if nlr == self:
                raise NextLessRestrictiveCycleError
    
    def __eq__(self, other):
        if isinstance(other, TableColumnType) or issubclass(type(other), TableColumnType):
            return self.name == other.name
        else:
            return other.__eq__(self)

    def __lt__(self, other):
        nlr = other
        while nlr:
            nlr = nlr.next_less_restrictive
            if nlr == self:
                return True
        return False
        
    def __gt__(self, other):
        return other < self

    def is_value_compatible(self, value):
        """Checks to see if the given value can be inserted into a column of this type.

        This should be implemented by subclasses with db-specific logic.

        Args:
          value: The value to check, a string
        Returns:
          True if this string could be inserted into a column of this type, else False
        Raises:
          NotImplementedError
        """
        raise NotImplementedError

class TableColumn(NameSqlMixin, DictableMixin, object):
    """DB-agnostic base class for storing info about a column in a table.

    Attributes:
      name: The identifier for the column
      type: A TableColumnType instance with type information for the column.
    """

    def __init__(self, name, type):
        self.name = name
        self.type = type


class TableDefinition(DictableMixin, object):
    """DB-agnostic base class for storing info about a table

    Attributes:
      name: The identifier for this table.
      columns: A list of TableColumns describing this table's columns.
    """

    def __init__(self, name, columns):
        # TODO
        raise NotImplementedError
    
    def create_sql(self):
        """Generate a sql statement for creating a table based on this TableDefinition.
        Subclasses should implement this.
        """
        raise NotImplementedError

    def identifier_string(self):
        """Get the string representation of the identifier for this table."""
        return self.name

    def add_column(self, column):
        """Add a column to the column list."""
        for col in self.columns:
            if col.name == column.name:
                raise ValueError(
                    "Column with name {} already exists in TableDefinition {}".format(
                        column.name, self.identifier_string()))
    def update_column(self, column):
        """Update the existing column with name column.name to the given column"""
        #TODO

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
    table_def = TableDefinition
        
    def get_type(self, value, previous_type=None):
        """Get what type of column the given value would be.
        
        Args:
          value: the value to get the type for.
          previous_type: the type that the column this value is in
                         had previously been assigned
        Returns:
          A TableColumnType that the given value is compatible with.
        """
        
        raise NotImplementedError

    def column_types(self):
        """Iterate through column types in this Schematic
        
        Yields:
          A TableColumnType
        """
        #TODO
        raise NotImplementedError

    def column_type_from_name(self, name):
        """Get the TableColumnTypeInstance described by the given name.
        
        Args:
          name: The name, e.g. 'VARCHAR(256)'
        
        Returns:
          A TableColumnTypeInstance
        """
        #TODO
        raise NotImplementedError
