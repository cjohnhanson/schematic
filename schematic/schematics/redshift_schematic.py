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

class RedshiftTableColumn(schematic.TableColumn, schematic.NameSqlMixin):
    """Redshift-specific implementation of TableColumn

    Attributes:
      distkey: Boolean indicating whether or not this is the distkey
               for the table.
      sortkey: Integer indicating which sortkey this is for the table.
               If this column isn't a sortkey, this is None.
      encoding: The encoding of this tables values.
      notnull: Whether or not this column has a NOT NULL constraint.
    """

    def __init__(self, name, column_type, distkey=False, sortkey=None, encoding=None, notnull=False):
        super().__init__(name, column_type)
        self.distkey = distkey
        self.sortkey = sortkey
        self.encoding = encoding
        self.notnull = notnull


class RedshiftVarcharType(schematic.TableColumnType):
    """A Varchar type in Redshift.

    Attributes:
      parameter: int. The maximum length (in bytes) that can fit
               in a column of this type.
    """
    name = "RedshiftVarcharType"
    next_less_restrictive = None
    parameterized = True

    def __init__(self, parameter=1):
        super(RedshiftVarcharType, self).__init__(parameter=parameter)
        if self.parameter > RedshiftSchematic.MAX_CHAR_BYTES:
            raise ValueError(
                "Value too large for parameter. VARCHAR columns can have a length of at most {}".format(
                    RedshiftSchematic.MAX_CHAR_BYTES))

    def to_sql(self):
        return "VARCHAR ({})".format(self.parameter)

    def is_value_compatible_with_instance(self, value):
        """Determine if value can be inserted into column of
           type described by the instance.

        Args:
          value: The value to check.
        """
        return self.get_parameter_for_value(value) <= self.parameter

    def is_value_compatible_with_class(self, value):
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

    def __init__(self, parameter=1):
        super(RedshiftCharType, self).__init__(parameter)

    def to_sql(self):
        return "CHAR ({})".format(self.parameter)

    def is_value_compatible_with_instance(self, value):
        return len(
            str(value).encode("utf-8")) == len(
            str(value)) and super(
            RedshiftCharType,
            self).is_value_compatible_with_instance(value)

    def is_value_compatible_with_class(self, value):
        return len(
            str(value).encode("utf-8")) == len(
            str(value)) and super(
            RedshiftCharType,
            self).is_value_compatible_with_class(value)


class RedshiftBooleanType(schematic.TableColumnType):
    """A boolean type in Redshift"""
    name = "RedshiftBooleanType"
    next_less_restrictive = RedshiftVarcharType
    parameterized = False
    valid_true_literals = ['TRUE', 't', 'true', 'y', 'yes', '1']
    valid_false_literals = ['FALSE', 'f', 'false', 'n', 'no', '0']

    def __init__(self):
        super(RedshiftBooleanType, self).__init__()

    def to_sql(self):
        return "BOOLEAN"

    def is_value_compatible_with_class(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.

        Args:
          value: The value to check.
        """
        return value in self.valid_false_literals or value in self.valid_true_literals


class RedshiftAbstractDatetimeType(schematic.TableColumnType):
    """Abstract datetime type to provide subclasses compatibility
    checking logic.

    Attributes:
      valid_regex: Any valid value for columns of the subclass's
                   type will match this regex
    """
    valid_regex = None

    def is_value_compatible_with_class(self, value):
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
    valid_regex = re.compile("^({})({})$".format(VALID_DATE_PATTERN,
                                                 VALID_TIME_PATTERN))

    def __init__(self):
        super(RedshiftTimestampType, self).__init__()

    def to_sql(self):
        return "TIMESTAMP"


class RedshiftTimestampTZType(RedshiftAbstractDatetimeType):
    """A Timestamp with time zone type in Redshift"""
    name = "RedshiftTimestampTZType"
    next_less_restrictive = RedshiftTimestampType
    parameterized = False
    valid_regex = re.compile("^({})({})({})$".format(VALID_DATE_PATTERN,
                                                     VALID_TIME_PATTERN,
                                                     VALID_TIMEZONE_PATTERN))

    def __init__(self):
        super(RedshiftTimestampTZType, self).__init__()

    def to_sql(self):
        return "TIMESTAMPTZ"


class RedshiftDateType(RedshiftAbstractDatetimeType):
    """A DATE type in Redshift"""
    name = "RedshiftDateType"
    next_less_restrictive = RedshiftTimestampTZType
    parameterized = False
    valid_regex = re.compile("^({})$".format(VALID_DATE_PATTERN))

    def __init__(self):
        super(RedshiftDateType, self).__init__()

    def to_sql(self):
        return "DATE"


class RedshiftAbstractDecimalType(schematic.TableColumnType):
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

    def __init__(self, parameter=(1, 1)):
        super(RedshiftDecimalType, self).__init__()
        self.precision, self.scale = parameter

    def to_sql(self):
        return "DECIMAL({}, {})".format(self.precision, self.scale)

    def is_value_compatible_with_instance(self, value):
        """Determine if value can be inserted into column of
           type described by the instance.

        Args:
          value: The value to check.
        """
        return self.check_compatible(value)

    def is_value_compatible_with_class(self, value):
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

    def __init__(self):
        super(RedshiftDoublePrecisionType, self).__init__()

    def to_sql(self):
        return "DOUBLE PRECISION"

    def is_value_compatible_with_class(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.

        Args:
          value: The value to check.
        """
        return self.check_compatible(value)


class RedshiftFloatType(RedshiftAbstractDecimalType):
    """An float type in Redshift"""
    name = "RedshiftFloatType"
    next_less_restrictive = RedshiftDoublePrecisionType
    parameterized = False
    precision = 6
    scale = 6

    def __init__(self):
        super(RedshiftFloatType, self).__init__()

    def to_sql(self):
        return "FLOAT"

    def is_value_compatible_with_class(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.

        Args:
          value: The value to check.
        """
        return self.check_compatible(value)


class RedshiftAbstractIntType(schematic.TableColumnType):
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

    def is_value_compatible_with_class(self, value):
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
    next_less_restrictive = RedshiftFloatType
    min_value = -9223372036854775808
    max_value = 9223372036854775807

    def __init__(self):
        super(RedshiftBigIntType, self).__init__()

    def to_sql(self):
        return "BIGINT"


class RedshiftIntType(RedshiftAbstractIntType):
    """An int type in Redshift"""
    name = "RedshiftIntType"
    next_less_restrictive = RedshiftBigIntType
    parameterized = False
    min_value = -2147483648
    max_value = 2147483647

    def __init__(self):
        super(RedshiftIntType, self).__init__()

    def to_sql(self):
        return "INT"


class RedshiftSmallIntType(RedshiftAbstractIntType):
    """A smallint type in Redshift"""
    name = "RedshiftSmallIntType"
    next_less_restrictive = RedshiftIntType
    parameterized = False
    min_value = -32768
    max_value = 32767

    def __init__(self):
        super(RedshiftSmallIntType, self).__init__()

    def to_sql(self):
        return "SMALLINT"


class RedshiftTableDefinition(schematic.TableDefinition):
    """Redshift-specific implementation of TableDefinition"""

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
        """Instantiate from a redshift cursor"""
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

    def create_sql(self):
        """Generate a sql statement for creating a table based
        on this RedshiftTableDefinition in Redshift

        Returns:
          A psycopg2.sql.SQL object for creating a table in Redshift
          based on this RedshiftTableDefinition.
        """
        # TODO
        raise NotImplementedError

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
                              RedshiftSmallIntType,
                              RedshiftDateType]
    table_def = RedshiftTableDefinition
    MAX_VARCHAR_BYTES = 65535
    MAX_CHAR_BYTES = 65535
