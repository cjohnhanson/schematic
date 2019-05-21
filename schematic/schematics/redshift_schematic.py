import schematic

class RedshiftTableColumn(schematic.TableColumn, schematic.NameSqlMixin):
    """Redshift-specific implementation of TableColumn"""

    def __init__(self, name, type, distkey, sortkey, encoding, notnull):
        super(RedshiftTableColumn, self).__init__(name, type)
        self.distkey = distkey
        self.sortkey = sortkey
        self.encoding = encoding
        self.notnull = notnull

class RedshiftVarcharType(schematic.TableColumnType):
    """A Varchar type in Redshift.
    
    Attributes:
      max_len: int. The maximum length (in bytes) that can fit
               in a column of this type.
    """
    name = "RedshiftVarcharType"
    next_less_restrictive = None
    name_regex = None #TODO
    
    def __init__(self, max_len):
        super(RedshiftVarcharType, self).__init__()
        self.max_len = max_len

    def to_sql(self):
        return "VARCHAR ({})".format(self.max_len)

    def is_value_compatible_with_instance(self, value):
        """Determine if value can be inserted into column of
           type described by the instance.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError
    
    def is_value_compatible_with_class(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError

class RedshiftCharType(schematic.TableColumnType):
    """A Char type in Redshift"""
    name = "RedshiftCharType"
    next_less_restrictive = RedshiftVarcharType
    name_regex = None #TODO

    def __init__(self, len):
        super(RedshiftCharType, self).__init__()
        self.len = len

    def to_sql(self):
        return "CHAR ({})".format(self.len)

    def is_value_compatible_with_instance(self, value):
        """Determine if value can be inserted into column of
           type described by the instance.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError
    
    def is_value_compatible_with_class(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError

class RedshiftBooleanType(schematic.TableColumnType):
    """A boolean type in Redshift"""
    name = "RedshiftBooleanType"
    next_less_restrictive = RedshiftCharType
    name_regex = None #TODO
    
    def __init__(self, len):
        super(RedshiftCharType, self).__init__()
        self.len = len

    def to_sql(self):
        return "BOOLEAN".format(self.len)

    def is_value_compatible_with_instance(self, value):
        """Determine if value can be inserted into column of
           type described by the instance.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError
    
    def is_value_compatible_with_class(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError

class RedshiftTimestampType(schematic.TableColumnType):
    """A timestamp type in Redshift"""
    name = "RedshiftTimestampType"
    next_less_restrictive = RedshiftCharType
    name_regex = None #TODO
    
    def __init__(self, len):
        super(RedshiftTimestampType, self).__init__()

    def to_sql(self):
        return "TIMESTAMP".format(self.len)

    def is_value_compatible_with_instance(self, value):
        """Determine if value can be inserted into column of
           type described by the instance.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError
    
    def is_value_compatible_with_class(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError

class RedshiftTimestampType(schematic.TableColumnType):
    """A timestamp type in Redshift"""
    name = "RedshiftTimestampType"
    next_less_restrictive = RedshiftVarcharType
    name_regex = None #TODO
    
    def __init__(self, len):
        super(RedshiftTimestampType, self).__init__()

    def to_sql(self):
        return "TIMESTAMP".format(self.len)

    def is_value_compatible_with_instance(self, value):
        """Determine if value can be inserted into column of
           type described by the instance.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError
    
    def is_value_compatible_with_class(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError

class RedshiftTimestampTZType(schematic.TableColumnType):
    """A Timestamp with time zone type in Redshift"""
    name = "RedshiftTimestampTZType"
    next_less_restrictive = RedshiftTimestampType
    name_regex = None #TODO
    
    def __init__(self):
        super(RedshiftTimestampTZType, self).__init__()

    def to_sql(self):
        return "TIMESTAMPTZ".format(self.len)

    def is_value_compatible_with_instance(self, value):
        """Determine if value can be inserted into column of
           type described by the instance.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError
    
    def is_value_compatible_with_class(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError

class RedshiftBooleanType(schematic.TableColumnType):
    """A boolean type in Redshift"""
    name = "RedshiftBooleanType"
    next_less_restrictive = RedshiftCharType
    name_regex = None #TODO
    
    def __init__(self):
        super(RedshiftCharType, self).__init__()

    def to_sql(self):
        return "BOOLEAN".format(self.len)

    def is_value_compatible_with_instance(self, value):
        """Determine if value can be inserted into column of
           type described by the instance.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError
    
    def is_value_compatible_with_class(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError

class RedshiftDecimalType(schematic.TableColumnType):
    """A decimal type in Redshift"""
    name = "RedshiftDecimalType"
    next_less_restrictive = RedshiftCharType
    name_regex = None #TODO
    
    def __init__(self, precision, scale):
        super(RedshiftDecimalType, self).__init__()
        self.precision = precision
        self.scale = scale

    def to_sql(self):
        return "DECIMAL({}, {})".format(self.precision, self.scale)

    def is_value_compatible_with_instance(self, value):
        """Determine if value can be inserted into column of
           type described by the instance.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError
    
    def is_value_compatible_with_class(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError

class RedshiftDoublePrecisionType(schematic.TableColumnType):
    """An double precision type in Redshift"""
    name = "RedshiftDoublePrecisionType"
    next_less_restrictive = RedshiftDecimalType
    name_regex = None #TODO
    
    def __init__(self):
        super(RedshiftDoublePrecisionType, self).__init__()

    def to_sql(self):
        return "DOUBLE PRECISION".format(self.len)

    def is_value_compatible_with_instance(self, value):
        """Determine if value can be inserted into column of
           type described by the instance.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError
    
    def is_value_compatible_with_class(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError

class RedshiftFloatType(schematic.TableColumnType):
    """An float type in Redshift"""
    name = "RedshiftFloatType"
    next_less_restrictive = RedshiftDoublePrecisionType
    name_regex = None #TODO
    
    def __init__(self):
        super(RedshiftFloatType, self).__init__()

    def to_sql(self):
        return "FLOAT".format(self.len)

    def is_value_compatible_with_instance(self, value):
        """Determine if value can be inserted into column of
           type described by the instance.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError
    
    def is_value_compatible_with_class(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError

class RedshiftBigIntType(schematic.TableColumnType):
    """An bigint type in Redshift"""
    name = "RedshiftBigintType"
    next_less_restrictive = RedshiftFloatType
    name_regex = None #TODO
    
    def __init__(self):
        super(RedshiftBigintType, self).__init__()

    def to_sql(self):
        return "BIGINT".format(self.len)

    def is_value_compatible_with_instance(self, value):
        """Determine if value can be inserted into column of
           type described by the instance.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError
    
    def is_value_compatible_with_class(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError

class RedshiftIntType(schematic.TableColumnType):
    """An int type in Redshift"""
    name = "RedshiftIntType"
    next_less_restrictive = RedshiftBigIntType
    name_regex = None #TODO
    
    def __init__(self):
        super(RedshiftIntType, self).__init__()

    def to_sql(self):
        return "INT".format(self.len)

    def is_value_compatible_with_instance(self, value):
        """Determine if value can be inserted into column of
           type described by the instance.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError
    
    def is_value_compatible_with_class(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError

class RedshiftSmallIntType(schematic.TableColumnType):
    """A smallint type in Redshift"""
    name = "RedshiftSmallIntType"
    next_less_restrictive = RedshiftIntType
    name_regex = None #TODO
    
    def __init__(self):
        super(RedshiftSmallIntType, self).__init__()

    def to_sql(self):
        return "SMALLINT".format(self.len)

    def is_value_compatible_with_instance(self, value):
        """Determine if value can be inserted into column of
           type described by the instance.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError
    
    def is_value_compatible_with_class(self, value):
        """Determine if value can be inserted into column of
           the group of types described by the class.
        
        Args:
          value: The value to check.
        """
        #TODO
        raise NotImplementedError

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


class RedshiftSchematic(schematic.Schematic):
    """Redshift-specific implementation of Schematic.

    Attributes:
      name: Static attribute with the name of this schematic
      column_types: 2-D Array of TableColumnTypes
      table_def: implementation of TableDefinition for this schematic
    """
    name = 'redshift'
    column_types = []  # TODO
    table_def = RedshiftTableDefinition
