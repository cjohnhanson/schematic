import schematic


class RedshiftTableColumn(schematic.TableColumn, schematic.NameSqlMixin):
    """Redshift-specific implementation of TableColumn"""

    def __init__(self, name, type, distkey, sortkey, encoding, notnull):
        super(RedshiftTableColumn, self).__init__(name, type)
        self.distkey = distkey
        self.sortkey = sortkey
        self.encoding = encoding
        self.notnull = notnull

    def identifier_string(self):
        return name


class RedshiftVarcharType(schematic.TableColumnType):
    """A Varchar type in Redshift"""
    name = "RedshiftVarcharType"
    next_less_restrictive = None

    def __init__(self, max_len):
        super(RedshiftVarcharType, self).__init__()
        self.max_len = max_len

    def to_sql(self):
        return "VARCHAR ({})".format(self.max_len)

    def is_value_compatible(value):
        # TODO
        raise NotImplementedError


class RedshiftCharType(schematic.TableColumnType):
    """A Char type in Redshift"""
    name = "RedshiftCharType"
    next_less_restrictive = RedshiftVarcharType

    def __init__(self, len):
        self.len = len

    def to_sql(self):
        return "CHAR ({})".format(self.len)


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
