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

"""
Implementation of the schematic.Schematic interface
with Redshift-specific logic and Schematic.TableColumnType
implementations.

Based on Redshift documentation:
- https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html

TODO(Cody): Determine how/whether to handle CHAR types (currently defaults to VARCHAR)
TODO(Cody): Determine how/whether to handle NCHAR and NVARCHAR types
TODO(Cody): Implement how/whether to handle BPCHAR and TEXT types
TODO(Cody): Get the datetime regexes 1:1 with Redshift's datetime logic
"""
import schematic
import re
from psycopg2 import sql

VALID_DATE_PATTERNS = [
    r"([0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|1[1-9]|2[1-9]|3[0-1]))",
    r"(([0-9]{4})(0[1-9]|1[0-2])(0[1-9]|2[1-9]|3[0-1]))",
    r"((0[1-9]|1[0-2])/(0[1-9]|2[1-9]|3[0-1]|[1-9])/([0-9]{2}|[0-9]{4}))",
    r"(today)",
    r"(tomorrow)",
    r"(yesterday)"]
VALID_DATE_PATTERN = r"({})".format(r"|".join(VALID_DATE_PATTERNS))
VALID_TIME_PATTERNS = [
    r"((T| )(((([0-1][0-9])|(2[0-3])):([0-5][0-9])(:([0-5][0-9]))?(\.[0-9])*)))",
    r"((T| )((0[1-9]|1[0-2]):([0-5][0-9])(:([0-5][0-9]))?(\.[0-9]*)? (AM|PM)))"
]
VALID_TIME_PATTERN = r"({})".format("|".join(VALID_TIME_PATTERNS))
VALID_TIMEZONE_PATTERNS = [
    r"(\+(0[1-9]|1[0-2]):00)"
]
VALID_TIMEZONE_PATTERN = r"({})".format("|".join(VALID_TIMEZONE_PATTERNS))
DEFAULT_NULL_STRINGS = ["", "None", "Null"]


class RedshiftTableColumn(schematic.TableColumn, schematic.NameSqlMixin):
    """Redshift-specific implementation of TableColumn

    Attributes:
      distkey: Boolean indicating whether or not this is the distkey
               for the table.
      sortkey: Integer indicating which sortkey this is for the table.
               If this column isn't a sortkey, this is None.
      encoding: The encoding of this tables values.
      notnull: Whether or not this column has a NOT NULL constraint.
      primary_key: Optimization hint for Redshift query planner, boolean
      unique: Optimization hint for Redshift query planner, boolean
    """

    def __init__(self,
                 name,
                 column_type,
                 distkey=False,
                 sortkey=None,
                 encoding=None,
                 notnull=False,
                 primary_key=False,
                 unique=False):
        super().__init__(name, column_type=column_type)
        self.distkey = distkey
        self.sortkey = sortkey
        self.encoding = encoding
        self.notnull = notnull
        self.primary_key = primary_key
        self.unique = unique

    def create_sql(self):
        """psycopg2.sql for this column in a CREATE TABLE statement

        Returns:
           A psycopg2.sql object
        """
        return sql.SQL("{name} {column_type}").format(
            name=sql.Identifier(
                self.name), column_type=self.column_type.to_sql())


class RedshiftTableColumnType(schematic.TableColumnType):
    """Base class for all Redshift-specific TableColumnType
    implementations.

    Attributes:
      def_regex: a regex to match against the "type" column in pg_table_def
    """
    def_regex = None

    @classmethod
    def from_pg_table_def(cls, type_string):
        """Instantiate from the string in the "type" column of pg_table_def.
        Returns:
          A RedshiftTableColumnType instance
        Raises:
          ValueError: if the type_string can't be matched by the def_regex for the class
        """
        search = cls.def_regex.search(type_string)
        if not search:
            raise ValueError(
                "{} is not a valid type string for {}".format(
                    type_string, cls.name))
        if cls.parameterized:
            return cls(parameter=search.group(1))
        else:
            return(cls())




class RedshiftVarcharType(RedshiftTableColumnType):
    """A Varchar type in Redshift.

    Attributes:
      parameter: int. The maximum length (in bytes) that can fit
                 in a column of this type.
    """
    name = "RedshiftVarcharType"
    next_less_restrictive = None
    parameterized = True
    def_regex = re.compile(r"character varying\(([0-9]+)\)")

    def __init__(self, parameter=1):
        super(RedshiftVarcharType, self).__init__(parameter=int(parameter))
        if self.parameter > RedshiftSchematic.MAX_CHAR_BYTES:
            raise ValueError(
                "Value too large for parameter. VARCHAR columns can have a length of at most {}".format(
                    RedshiftSchematic.MAX_CHAR_BYTES))

    def to_sql(self):
        return sql.SQL("VARCHAR ({})".format(self.parameter))

    def value_is_compatible(self, value):
        """Determine if value can be inserted into column of
           type described by the instance.

        Args:
          value: The value to check.
        """
        return self.get_parameter_for_value(value) <= self.parameter

    def _value_is_compatible_superset(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.

        Args:
          value: The value to check.
        """
        return self.get_parameter_for_value(
            value) <= RedshiftSchematic.MAX_CHAR_BYTES

    @staticmethod
    def get_parameter_for_value(value):
        """Get the parameter for a column of this type
           which can contain the given value.

        Args:
          value: The value to check.
        Returns:
          The parameter which fits the given value
        """
        return len(str(value).encode('utf-8'))


class RedshiftCharType(RedshiftVarcharType):
    """A Char type in Redshift.

    Attributes:
      parameter: The number of bytes that can fit into a column of this type.
    """
    name = "RedshiftCharType"
    next_less_restrictive = RedshiftVarcharType
    def_regex = re.compile(r"character\(([0-9]+)\)")

    def __init__(self, parameter=1):
        super(RedshiftCharType, self).__init__(int(parameter))

    def to_sql(self):
        return sql.SQL("CHAR ({})".format(self.parameter))

    def value_is_compatible(self, value):
        return len(
            str(value).encode("utf-8")) == len(
            str(value)) and super(
            RedshiftCharType,
            self).value_is_compatible(value)

    def _value_is_compatible_superset(self, value):
        return len(
            str(value).encode("utf-8")) == len(
            str(value)) and super(
            RedshiftCharType,
            self)._value_is_compatible_superset(value)


class RedshiftAbstractDatetimeType(RedshiftTableColumnType):
    """Abstract datetime type to provide subclasses compatibility
    checking logic.

    Attributes:
      valid_regex: Any valid value for columns of the subclass's
                   type will match this regex
    """
    valid_regex = None

    def _value_is_compatible_superset(self, value):
        """Check to see if a given value could be
        inserted into a column of this type.

        Args:
          value: The value to check
        Returns:
          boolean indicating whether or not the value
          is compatible
        """
        return bool(self.valid_regex.match(value))


class RedshiftTimestampType(RedshiftAbstractDatetimeType):
    """A timestamp type in Redshift"""
    name = "RedshiftTimestampType"
    next_less_restrictive = RedshiftVarcharType
    parameterized = False
    def_regex = re.compile(r"timestamp without time zone")
    valid_regex = re.compile("^({})({})$".format(VALID_DATE_PATTERN,
                                                 VALID_TIME_PATTERN))

    def __init__(self):
        super(RedshiftTimestampType, self).__init__()

    def to_sql(self):
        return sql.SQL("TIMESTAMP")


class RedshiftTimestampTZType(RedshiftAbstractDatetimeType):
    """A Timestamp with time zone type in Redshift"""
    name = "RedshiftTimestampTZType"
    next_less_restrictive = RedshiftTimestampType
    parameterized = False
    def_regex = re.compile(r"timestamp with time zone")
    valid_regex = re.compile("^({})({})({})$".format(VALID_DATE_PATTERN,
                                                     VALID_TIME_PATTERN,
                                                     VALID_TIMEZONE_PATTERN))

    def __init__(self):
        super(RedshiftTimestampTZType, self).__init__()

    def to_sql(self):
        return sql.SQL("TIMESTAMPTZ")


class RedshiftDateType(RedshiftAbstractDatetimeType):
    """A DATE type in Redshift"""
    name = "RedshiftDateType"
    next_less_restrictive = RedshiftTimestampTZType
    parameterized = False
    def_regex = re.compile(r"date")
    valid_regex = re.compile("^({})$".format(VALID_DATE_PATTERN))

    def __init__(self):
        super(RedshiftDateType, self).__init__()

    def to_sql(self):
        return sql.SQL("DATE")


class RedshiftAbstractDecimalType(RedshiftTableColumnType):
    """Abstract decimal type to provide subclasses compatibility
    checking logic.

    Attributes:
      scale: Total number of digits that can fit into a column of this type.
      precision: Number of digits to right of the decimal point that can
                 fit into a column of this type.
    """

    def check_compatible(self,
                         value,
                         precision=None,
                         scale=None):
        """Check to see if a value is compatible with a column
        with this precision and scale.

        Args:
          value: the value to check.
        Returns:
          Boolean indicating compatibility.
        """
        if not precision:
            precision_to_check = self.precision
        else:
            precision_to_check = precision
        if not scale:
            scale_to_check = self.scale
        else:
            scale_to_check = scale
        try:
            float(value)
        except ValueError:
            return False
        split_at_decimal = str(value).split(".")
        if len(split_at_decimal) == 1:
            split_at_decimal.append("")
        return (len(split_at_decimal[1]) <= scale_to_check and
                len("".join(split_at_decimal)) <= precision_to_check)


class RedshiftDecimalType(RedshiftAbstractDecimalType):
    """A decimal type in Redshift"""
    name = "RedshiftDecimalType"
    next_less_restrictive = RedshiftVarcharType
    parameterized = True
    max_scale = 37
    max_precision = 38
    def_regex = re.compile(r"numeric\(([0-9]+),([0-9]+)\)")

    def __init__(self, parameter=(1, 1)):
        super(RedshiftDecimalType, self).__init__()
        self.precision, self.scale = int(parameter[0]), int(parameter[1])

    @classmethod
    def from_pg_table_def(cls, type_string):
        search = cls.def_regex.search(type_string)
        if not search:
            raise ValueError(
                "{} is not a valid type string for {}".format(
                    type_string, cls.name))
        return cls(parameter=search.groups())

    def to_sql(self):
        return sql.SQL("DECIMAL({}, {})".format(self.precision, self.scale))

    def value_is_compatible(self, value):
        """Determine if value can be inserted into column of
           type described by the instance.

        Args:
          value: The value to check.
        """
        return self.check_compatible(value)

    def _value_is_compatible_superset(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.

        Args:
          value: The value to check.
        """
        return self.check_compatible(value,
                                     scale=self.max_scale,
                                     precision=self.max_precision)

    @staticmethod
    def get_parameter_for_value(value):
        split_at_decimal = str(value).split(".")
        if len(split_at_decimal) == 1:
            split_at_decimal.append("")
        return (len("".join(split_at_decimal)),
                len(split_at_decimal[1]))


class RedshiftDoublePrecisionType(RedshiftAbstractDecimalType):
    """An double precision type in Redshift"""
    name = "RedshiftDoublePrecisionType"
    next_less_restrictive = RedshiftDecimalType
    parameterized = False
    precision = 15
    scale = 15
    def_regex = re.compile(r"double precision")

    def __init__(self):
        super(RedshiftDoublePrecisionType, self).__init__()

    def to_sql(self):
        return sql.SQL("DOUBLE PRECISION")

    def _value_is_compatible_superset(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.

        Args:
          value: The value to check.
        """
        return self.check_compatible(value)


class RedshiftRealType(RedshiftAbstractDecimalType):
    """An real type in Redshift"""
    name = "RedshiftRealType"
    next_less_restrictive = RedshiftDoublePrecisionType
    parameterized = False
    precision = 6
    scale = 6
    def_regex = re.compile(r"real")

    def __init__(self):
        super(RedshiftRealType, self).__init__()

    def to_sql(self):
        return sql.SQL("REAL")

    def _value_is_compatible_superset(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.

        Args:
          value: The value to check.
        """
        return self.check_compatible(value)


class RedshiftAbstractIntType(RedshiftTableColumnType):
    """Abstract int type to provide subclasses compatibility
    checking logic.

    Attributes:
      min_value: Total number of digits that can fit into a column of this type.
      max_value: Number of digits to right of the decimal point that can
                 fit into a column of this type.
    """
    parameterized = False
    min_value = None
    max_value = None

    def _value_is_compatible_superset(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.

        Args:
          value: The value to check.
        """
        try:
            cast_value = float(value)
        except ValueError:
            return False
        return (cast_value >= self.min_value and
                cast_value <= self.max_value and
                cast_value // 1 == cast_value)


class RedshiftBigIntType(RedshiftAbstractIntType):
    """An bigint type in Redshift"""
    name = "RedshiftBigIntType"
    next_less_restrictive = RedshiftRealType
    min_value = -9223372036854775808
    max_value = 9223372036854775807
    def_regex = re.compile(r"bigint")

    def __init__(self):
        super(RedshiftBigIntType, self).__init__()

    def to_sql(self):
        return sql.SQL("BIGINT")


class RedshiftIntType(RedshiftAbstractIntType):
    """An int type in Redshift"""
    name = "RedshiftIntType"
    next_less_restrictive = RedshiftBigIntType
    parameterized = False
    min_value = -2147483648
    max_value = 2147483647
    def_regex = re.compile(r"int")

    def __init__(self):
        super(RedshiftIntType, self).__init__()

    def to_sql(self):
        return sql.SQL("INT")


class RedshiftSmallIntType(RedshiftAbstractIntType):
    """A smallint type in Redshift"""
    name = "RedshiftSmallIntType"
    next_less_restrictive = RedshiftIntType
    parameterized = False
    min_value = -32768
    max_value = 32767
    def_regex = re.compile(r"smallint")

    def __init__(self):
        super(RedshiftSmallIntType, self).__init__()

    def to_sql(self):
        return sql.SQL("SMALLINT")


class RedshiftBooleanType(RedshiftTableColumnType):
    """A boolean type in Redshift"""
    name = "RedshiftBooleanType"
    next_less_restrictive = RedshiftBigIntType
    parameterized = False
    valid_true_literals = ['TRUE', 't', 'true', 'y', 'yes', '1']
    valid_false_literals = ['FALSE', 'f', 'false', 'n', 'no', '0']
    def_regex = re.compile(r"boolean")

    def __init__(self):
        super(RedshiftBooleanType, self).__init__()

    def to_sql(self):
        return sql.SQL("BOOLEAN")

    def _value_is_compatible_superset(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.

        Args:
          value: The value to check.
        """
        return value in self.valid_false_literals or value in self.valid_true_literals


class RedshiftTableDefinition(schematic.TableDefinition):
    """Redshift-specific implementation of TableDefinition"""

    def __init__(self, schema, name, columns):
        self.schema = schema
        self.tablename = name
        self.name = "{}.{}".format(schema, name)
        self.columns = columns
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
        """Instantiate from a redshift connection"

        Args:
          conn: A psycopg2.connection to a Redshift instance
          schema: The name of the schema where the table resides
          name: The name of the table to create a definition for
        Returns:
          A RedshiftTableDefinition object
        Raises:
          psycopg2.ProgrammingError: if the specified table does not exist
          psycopg2.OperationalError: if there are connection/transaction issues

        """
        get_sql = sql.SQL("""
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
        """).format(schemaname=sql.Literal(schema),
                    tablename=sql.Literal(name))
        table_def = cls(schema=schema,
                        name=name,
                        columns=[])
        with conn.cursor() as curs:
            curs.execute(get_sql)
            for column, column_type, encoding, distkey, sortkey, notnull in curs.fetchall():
                table_def.add_column(
                    RedshiftTableColumn(
                        name=column,
                        column_type=RedshiftSchematic().get_type_from_string(column_type),
                        encoding=encoding,
                        distkey=distkey,
                        sortkey=sortkey,
                        notnull=notnull))
        return table_def

    def create_sql(self):
        """Generate a sql statement for creating a table based
        on this RedshiftTableDefinition in Redshift

        Returns:
          A psycopg2.sql.SQL object for creating a table in Redshift
          based on this RedshiftTableDefinition.
        Raises:
          schematic.NoColumnsError: if this table has no columns
        """
        if not self.columns:
            raise schematic.NoColumnsError
        columns_sql = sql.SQL(",").join(
            [col.create_sql() for col in self.columns])
        distkey_sql = sql.SQL("DISTKEY ({col})").format(
            col=sql.Identifier(self.distkey.name)) if self.distkey else sql.SQL("")
        sortkey_sql = sql.SQL("SORTKEY ({col})").format(col=sql.SQL(",").join(
            [sql.Identifier(column.name) for column in self.columns]))
        return sql.SQL("""CREATE TABLE IF NOT EXISTS {schema}.{tablename}
        ({columns})
        {distkey} {sortkey};""").format(schema=sql.Identifier(self.schema),
                                        tablename=sql.Identifier(self.tablename),
                                        columns=columns_sql,
                                        distkey=distkey_sql,
                                        sortkey=sortkey_sql)

    def create_table(self, conn):
        """Create the table based on this
        RedshiftTableDefinition in Redshift

        Args:
          conn: A psycopg2.connection to a Redshift instance
        Raises:
          psycopg2.OperationalError: If there's a connection or transaction issue
          psycopg2.ProgrammingError: If the table already exists
        """
        with conn.cursor() as curs:
            try:
                curs.execute(self.create_sql())
            except BaseException:
                conn.rollback()
                raise

    def get_rows(self, conn):
        """Get the rows in this table.
        Args:
          conn: A psycopg2.connection to a Redshift instance
        Raises:
          psycopg2.OperationalError: If there's a connection or transaction issue
          psycopg2.ProgrammingError: If the table already exists
        """
        raise NotImplementedError("TODO")


class RedshiftSchematic(schematic.Schematic):
    """Redshift-specific implementation of Schematic.

    Attributes:
      name: Static attribute with the name of this schematic
      column_types: 2-D Array of TableColumnTypes
      table_def: implementation of TableDefinition for this schematic
    """
    name = 'redshift'
    most_restrictive_types = [RedshiftVarcharType,
                              RedshiftBooleanType,
                              RedshiftDateType]
    table_definition_class = RedshiftTableDefinition
    column_class = RedshiftTableColumn
    MAX_VARCHAR_BYTES = 65535
    MAX_CHAR_BYTES = 65535
    null_strings = DEFAULT_NULL_STRINGS
    # TODO: BOOL -> BIGINT -> DOUBLE -> VARCHAR

    def get_type_from_string(self, type_string):
        """Get the RedshiftTableColumnType instance from
        a type string of the format that's in pg_table_def.

        Args:
          type_string: the string from pg_table_def
        Returns:
          A RedshiftTableColumnType instance
        """
        for column_type in self.column_types():
            try:
                return column_type().from_pg_table_def(type_string)
            except ValueError:
                continue
        raise ValueError(
            "No RedshiftTableColumnType matches {}".format(type_string))
