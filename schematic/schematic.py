#!/bin/env python
import click
from psycopg2 import sql
from csv import DictReader


class NameSqlMixin(object):
    """
    Mixin to provide default to_sql behavior
    """

    def to_sql(self):
        """
        :return: A SQL string representing this type
        :rtype: str
        """
        return self.name


class TableColumn(NameSqlMixin, object):
    """
    DB-agnostic base class for storing info about a column in a table
    """

    def __init__(self, name, type):
        self.name = name
        self.type = type


class TableDefinition(object):
    """
    DB-agnostic base class for storing info about a table
    """

    def __init__(self, name, columns):
        # TODO
        raise NotImplementedError

    @classmethod
    def from_csv(cls, csv_file):
        """
        Create a TableDefinition based on a CSV file
        """
        # TODO
        raise NotImplementedError

    def create_sql(self):
        """
        Generate a sql statement for creating a table based on this TableDefinition.
        Subclasses should implement this.
        """
        raise NotImplementedError

    def to_dict(self):
        """
        Get a dictionary which contains all relevant information for this table
        """
        # TODO
        raise NotImplementedError

    def identifier_string(self):
        """
        Return the string representation of the identifier for this table
        """
        return self.name

    def add_column(self, column):
        """
        Add a column to the column list
        """
        for col in self.columns:
            if col.name == column.name:
                raise ValueError(
                    "Column with name {} already exists in TableDefinition {}".format(
                        column.name, self.identifier_string()))

    @staticmethod
    def guess_type(value, name=None, type_dict={}):
        """
        :param value: The value to guess the type for
        :param name: The name of the column this value is being inserted into, if any
        :type name: str
        :param type_dict: A dictionary of previous best guesses for what the type of the column referenced by name would be
        :return: The best guess as to what the type of this value would be in a SQL database.
        :rtype: str
        """
        # TODO
        raise NotImplementedError


class RedshiftTableColumn(TableColumn, NameSqlMixin):
    """
    Redshift-specific implementation of TableColumn
    """

    def __init__(self, name, type, distkey, sortkey, encoding, notnull):
        super(RedshiftTableColumn, self).__init__(name, type)
        self.distkey = distkey
        self.sortkey = sortkey
        self.encoding = encoding
        self.notnull = notnull

    def identifier_string(self):
        return name


class TableColumnType(NameSqlMixin, object):
    """
    Represents a type for a table column
    """

    def __init__(self, name, priority):
        self.name = name
        self.priority = priority

    def is_value_compatible(self, value):
        """
        :return: Boolean indicating if the given value is convertible to this type.
        :param value: The value to check
        """
        raise NotImplementedError

    def get_next_less_restrictive_type(self):
        """
        :return: Return the subclass of TableColumnType which is less restrictive than this type
        :rtype: TableColumnType
        """
        raise NotImplementedError


class RedshiftVarcharType(TableColumnType):
    """
    A Varchar type in Redshift
    """

    def __init__(self, name, priority, max_len):
        super(RedshiftVarcharType, self).__init__(name, priority)
        self.max_len = max_len

    def to_sql(self):
        return "VARCHAR ({})".format(self.max_len)

    def is_value_compatible(value):
        # TODO


class RedshiftTableDefinition(TableDefinition):
    """
    Redshift-specific implementation of TableDefinition
    """

    def __init__(self, schema, name, columns):
        self.sortkeys = []
        sk_dict = {}
        self.distkey = None
        for col in columns:
            if col.distkey:
                self.distkey = col
            if col.sortkey:
                sk_dict[col.sortkey] = col
        for sk in sorted(sk_dict.keys()):
            self.sortkeys.append(sk_dict[sk])

    @classmethod
    def from_connection(cls, conn, schema, name):
        """
        Instantiate from a redshift cursor
        """
        get_sql = """
        SET search_path TO {schemaname};
        SELECT
          "column",
          "type",
          "encoding",
          "distkey",
          "sortkey",
          "notnull"
        FROM pg_catalog.pg_table_def
        WHERE schemaname = {schemaname}
          AND tablename = {tablename};
        """.format(schemaname=sql.Identifier(schema),
                   tablename=sql.Identifier(name))
        table_def = cls(schema=schema,
                        name=name,
                        columns=[])
        with conn.cursor() as curs:
            curs.execute(get_sql)
            for column, type, encoding, distkey, sortkey, notnull in curs.fetchall():
                table_def.add_column(
                    RedshiftTableColumn(
                        name=column,
                        type=type,
                        encoding=encoding,
                        distkey=distkey,
                        sortkey=sortkey,
                        notnull=notnull))
        return table_def

    @classmethod
    def from_csv(csv_file):
        """
        Instantiate from a CSV file
        """

    @staticmethod
    def guess_type(value, name=None, type_dict={}):
        """
        :param value: The value to guess the type for
        :param name: The name of the column this value is being inserted into, if any
        :type name: str
        :param type_dict: A dictionary of previous best guesses for what the type of the column referenced by name would be
        :return: The best guess as to what the type of this value would be in a SQL database.
        :rtype: str
        """
        # TODO


@click.group()
def cli():
    """Utilities for working with CSVs in a data warehouse"""
    # TODO
