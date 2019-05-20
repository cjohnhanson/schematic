import unittest
import schematic

class MockTableColumnType1(schematic.TableColumnType):
    name = "MockTableColumnType1"
    next_less_restrictive = None
        
class MockTableColumnType2(schematic.TableColumnType):
    name = "MockTableColumnType2"
    next_less_restrictive = MockTableColumnType1

class MockTableColumnType3(schematic.TableColumnType):
    name = "MockTableColumnType3"
    next_less_restrictive = MockTableColumnType1

class MockTableColumnType4(schematic.TableColumnType):
    name = "MockTableColumnType4"
    next_less_restrictive = MockTableColumnType1

class MockTableColumnType5(schematic.TableColumnType):
    name = "MockTableColumnType5"
    next_less_restrictive = MockTableColumnType2

class MockTableColumnType6(schematic.TableColumnType):
    name = "MockTableColumnType6"
    next_less_restrictive = MockTableColumnType5

class MockTableColumnType7(schematic.TableColumnType):
    name = "MockTableColumnType7"
    next_less_restrictive = MockTableColumnType6

class MockTableColumnType8(schematic.TableColumnType):
    name = "MockTableColumnType8"
    next_less_restrictive = MockTableColumnType3

class MockTableDefinitionClass(schematic.TableDefinition):
    pass
    
class MockSchematic(schematic.Schematic):
    name = "MockSchematic"
    most_restrictive_types = [MockTableColumnType8,
                              MockTableColumnType7,
                              MockTableColumnType4]
    table_definition_class = MockTableDefinitionClass

class TestTableColumnType(unittest.TestCase):
    """
    Test all the methods for the base TableColumnType class
    """

    def test_lt_returns_true_when_less_restrictive(self):
        self.assertTrue(MockTableColumnType1() < MockTableColumnType8())

    def test_lt_returns_false_when_false_when_more_restrictive(self):
        self.assertFalse(MockTableColumnType8() < MockTableColumnType1())

    def test_lt_returns_false_when_false_when_not_in_same_linked_list(self):
        self.assertFalse(MockTableColumnType8() < MockTableColumnType7())

    def test_gt_returns_true_when_more_restrictive(self):
        self.assertTrue(MockTableColumnType8() > MockTableColumnType1())

    def test_gt_returns_false_when_less_restrictive(self):
        self.assertFalse(MockTableColumnType1() > MockTableColumnType8())
        
    def test_gt_returns_false_when_not_in_same_linked_list(self):
        self.assertFalse(MockTableColumnType8() > MockTableColumnType7())

    def test_is_value_compatible_raises_notimplemented(self):
        with self.assertRaises(NotImplementedError):
            MockTableColumnType1().is_value_compatible('dummy')

class TestTableColumnMethods(unittest.TestCase):
    """
    Test all methods for the base TableColumn class
    """
    def test_repr(self):
        self.assertEqual(str(schematic.TableColumn("TestColumn1", MockTableColumnType1())),
                    "TestColumn1: MockTableColumnType1")

class TestTableDefinitionMethods(unittest.TestCase):
    """
    Test all the methods for the base TableDefinition class
    """

    def setUp(self):
        columns = [schematic.TableColumn("TestColumn1", MockTableColumnType1()),
                   schematic.TableColumn("TestColumn2", MockTableColumnType2()),
                   schematic.TableColumn("TestColumn3", MockTableColumnType3())]
        self.table_definition = schematic.TableDefinition("TestTableDefinition", columns)

    def test_create_sql_raises_notimplemented(self):
        with self.assertRaises(NotImplementedError):
            self.table_definition.create_sql()
        
    def test_identifier_string_returns_name(self):
        self.assertEqual(self.table_definition.identifier_string(), "TestTableDefinition")
            
    def test_add_new_column(self):
        new_column = schematic.TableColumn("TestColumn4", MockTableColumnType4())
        self.table_definition.add_column(new_column)
        self.assertIn(new_column, self.table_definition.columns)

    def test_add_existing_column(self):
        with self.assertRaises(ValueError):
            self.table_definition.add_column(self.table_definition.columns[0])

    def test_update_existing_column_updates(self):
        old_column = self.table_definition.columns[0]
        updated_column = schematic.TableColumn("TestColumn1", MockTableColumnType2)
        self.table_definition.update_column(updated_column)
        self.assertIn(updated_column, self.table_definition.columns)
        self.assertNotIn(old_column, self.table_definition.columns)

    def test_update_nonexistent_column(self):
        new_column = schematic.TableColumn("TestColumn9", MockTableColumnType8)
        with self.assertRaises(ValueError):
            self.table_definition.update_column(new_column)

class TestSchematic(unittest.TestCase):
    """
    Test all the methods for the base Schematic class
    """

    def test_get_type_raises_notimplemented(self):
        with self.assertRaises(NotImplementedError):
            schematic.Schematic().get_type("dummy")
    
    def test_column_types_yields_all(self):
        all_column_types = frozenset([str(MockTableColumnType1()),
                                      str(MockTableColumnType2()),
                                      str(MockTableColumnType3()),
                                      str(MockTableColumnType4()),
                                      str(MockTableColumnType5()),
                                      str(MockTableColumnType6()),
                                      str(MockTableColumnType7()),
                                      str(MockTableColumnType8())])
        yielded_column_types = frozenset([str(t) for t in MockSchematic().column_types()])
        self.assertEqual(all_column_types, yielded_column_types)
        
    def test_column_type_from_name_raises_notimplemented(self):
        with self.assertRaises(NotImplementedError):
            schematic.Schematic().column_type_from_name("dummy")
    
