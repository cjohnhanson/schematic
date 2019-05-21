import unittest
from schematic.schematics import redshift_schematic

class TestRedshiftTableColumnMethods(unittest.TestCase):
    """Test all methods for the RedshiftTableColumn class"""
    pass


class TestRedshiftTableDefinitionMethods(unittest.TestCase):
    """Test all the methods for the RedshiftTableDefinition class"""
    pass

class TestRedshiftVarcharTypeMethods(unittest.TestCase):
    """Test all the methods for the RedshiftVarcharType class"""

    def test_to_sql_returns_correct_string(self):
        self.assertEqual("VARCHAR (256)",
                         redshift_schematic.RedshiftVarcharType(256).to_sql())
        self.assertEqual("VARCHAR (2)",
                         redshift_schematic.RedshiftVarcharType(2).to_sql())

    def test_is_value_compatible_with_instance_returns_true(self):
        self.fail("TODO")

    def test_is_value_compatible_returns_false(self):
        self.fail("TODO")
        
class TestRedshiftSchematic(unittest.TestCase):
    """Test all the methods for the redshift Schematic class"""

    def test_get_type_not_implemented(self):
        self.fail("TODO")

    def test_column_types_yields_all(self):
        self.fail("TODO")
