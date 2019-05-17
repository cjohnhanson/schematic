#!/bin/env python
import click
from psycopg2 import sql
from csv import DictReader

class NameSqlMixin(object):
    """Mixin to provide default to_sql behavior."""

    def to_sql(self):
        """Get the SQL identifier string for the object.

        Returns:
          The name of the object
        """
        return name

class DictableMixin(object):
    """Mixin to provide marshaling to and from dict"""

    def to_dict(self):
        """Create a dictionary from this object"""
        return vars(self)

    @classmethod
    def from_dict(cls, class_dict, **kwargs):
        """Instantiate from a dictionary
        Args:
          class_dict: the dictionary containing the data for the class
          kwargs: additional keyword arguments required to instantiate this class
        """
        return cls(**{**class_dict, **kwargs})

    def to_file(self, handler, format, append=True, overwrite=False):
        """Persist this schema to a file.

        Args:
          handler: the file handler to write to
          format: json or yaml
          append: whether this table definition should be
                  appended to the end of the existing config, if any
          overwrite:  whether this table definition should be overwritten
                  if it already exists in the existing config
        """
        # TODO
        raise NotImplementedError

class TableColumnType(NameSqlMixin, DictableMixin, object):
    """Represents a type for a table column.

    Attributes:
      name: The name of the type
      priority: The priority level of this type.
    """

    def __init__(self, name, priority):
        self.name = name
        self.priority = priority

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

    def __init__(self, name: str, type: TableColumnType):
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

    @classmethod
    def from_csv(cls, csv_file, name=None):
        """Create a TableDefinition based on a CSV file

        Checks each row of the table and creates a TableDefinition
        instance with column names based on the header row
        and the most restrictive possible TableColumnType for each column.
        Args:
          csv_file: a file handler object for the CSV
          name: a table name for this TableDefinition. Defaults to file's basename
        Returns:
          A TableDefinition instance.
        Raises:
          NotImplementedError
        """
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

    @staticmethod
    def guess_type(value, name=None, type_dict={}):
        """The best guess as to what the type of this value would be in a SQL database.

        Args:
          value: The value to guess the type for
          name: The name of the column this value is being inserted into, if any
          type_dict: A dictionary of previous best guesses for what the type of the column referenced by name would be
        Returns:
          The best guess as to what the type of this value would be in a SQL database.
        """
        # TODO
        raise NotImplementedError

class Schematic(DictableMixin, object):
    """Implementation specifics for a type of database or warehouse
    
    Attributes:
      column_types: 2-D Array of TableColumnTypes
      table_def: implementation of TableDefinition for this schematic
    """
    def __init__(self):
        
        pass

    def get_type(self, value, previous_type=None):
        """Get what type of column the given value would be.
        
        Args:
          value: the value to get the type for.
          previous_type: the type that the column this value is in
                         had previously been assigned
        Returns:
          A TableColumnType that the given value is compatible with.
        """
        #TODO
        raise NotImplementedError

    def get_next_less_restrictive_type(self, column_type):
        """Get the TableColumnType which is less restrictive than the one given

        The next less restrictive type is the TableColumnType
        which is compatible with all the same values as this
        and is also compatible with the smallest number of additional values.
        E.g., in Redshift, a VARCHAR(MAX) would be the least restrictive type
        of all.
        
        Args:
          column_type: The ColumnTableType to compare
        Returns:
          A TableColumnType that is next less restrictive.
        Raises:
          NotImplementedError
        """
        #TODO
        raise NotImplementedError
    
    
