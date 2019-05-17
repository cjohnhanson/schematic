import schematic

class RedshiftTableColumn(schematic.TableColumn, schematic.NameSqlMixin):
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

class RedshiftVarcharType(schematic.TableColumnType):
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
        raise NotImplementedError

class RedshiftTableDefinition(schematic.TableDefinition):
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
