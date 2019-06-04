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
import unittest
import schematic


class MockTableColumnType1(schematic.TableColumnType):
    name = "MockTableColumnType1"
    next_less_restrictive = None
    parameterized = True

    def __init__(self, parameter=1):
        super(MockTableColumnType1, self).__init__(parameter=parameter)

    def value_is_compatible(self, value):
        if len(str(value)) > self.parameter:
            return False
        return True

    def _value_is_compatible_superset(self, value):
        return value != "BadValue"

    def get_parameter_for_value(value):
        return len(value)


class MockTableColumnType2(schematic.TableColumnType):
    name = "MockTableColumnType2"
    next_less_restrictive = MockTableColumnType1

    def _value_is_compatible_superset(self, value):
        return False


class MockTableColumnType3(schematic.TableColumnType):
    name = "MockTableColumnType3"
    next_less_restrictive = MockTableColumnType1

    def _value_is_compatible_superset(self, value):
        return value == "TestDeepest"


class MockTableColumnType4(schematic.TableColumnType):
    name = "MockTableColumnType4"
    next_less_restrictive = MockTableColumnType1

    def _value_is_compatible_superset(self, value):
        return False


class MockTableColumnType5(schematic.TableColumnType):
    name = "MockTableColumnType5"
    next_less_restrictive = MockTableColumnType2

    def _value_is_compatible_superset(self, value):
        return value == "TestDeepest"


class MockTableColumnType6(schematic.TableColumnType):
    name = "MockTableColumnType6"
    next_less_restrictive = MockTableColumnType5

    def _value_is_compatible_superset(self, value):
        return False


class MockTableColumnType7(schematic.TableColumnType):
    name = "MockTableColumnType7"
    next_less_restrictive = MockTableColumnType6

    def _value_is_compatible_superset(self, value):
        return False


class MockTableColumnType8(schematic.TableColumnType):
    name = "MockTableColumnType8"
    next_less_restrictive = MockTableColumnType3

    def _value_is_compatible_superset(self, value):
        return False

    def value_is_compatible(self, value):
        return False


class MockTableColumnTypeParameterized(schematic.TableColumnType):
    name = "MockTableColumnType9"
    parameterized = True


class MockTableDefinitionClass(schematic.TableDefinition):
    pass


class MockSchematic(schematic.Schematic):
    name = "MockSchematic"
    most_restrictive_types = [MockTableColumnType8,
                              MockTableColumnType7,
                              MockTableColumnType4]
    table_definition_class = MockTableDefinitionClass


class TestTableColumnTypeMethods(unittest.TestCase):
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

    def test_instantiation_with_parameter_raises_valueerror_non_parameterized(
            self):
        with self.assertRaises(ValueError):
            MockTableColumnType2("dummy parameter")

    def test_value_is_compatible_raises_notimplemented_base(
            self):
        with self.assertRaises(NotImplementedError):
            schematic.TableColumnType().value_is_compatible("dummy")

    def test_value_is_compatible_raises_notimplemented_parameterized(
            self):
        with self.assertRaises(NotImplementedError):
            MockTableColumnTypeParameterized().value_is_compatible("dummy")

    def test__value_is_compatible_superset_raises_notimplemented_base(self):
        with self.assertRaises(NotImplementedError):
            schematic.TableColumnType()._value_is_compatible_superset("dummy")

    def test_value_is_compatible_returns_true_parameterized(
            self):
        self.assertTrue(MockTableColumnType1(
            6).value_is_compatible('dummy'))

    def test_value_is_compatible_returns_true_not_parameterized(
            self):
        self.assertTrue(
            MockTableColumnType3().value_is_compatible("TestDeepest"))

    def test_get_parameter_for_value_raises_notimplemented_base(self):
        with self.assertRaises(NotImplementedError):
            schematic.TableColumnType.get_parameter_for_value("dummy")

    def test_value_is_compatible_returns_false_parameterized(
            self):
        self.assertFalse(MockTableColumnType1(
            3).value_is_compatible('dummy'))

    def test__value_is_compatible_superset_returns_true_unrestrictive(self):
        self.assertTrue(
            MockTableColumnType1()._value_is_compatible_superset('dummy'))

    def test__value_is_compatible_superset_raises_valueerror_no_compatible_type(
            self):
        with self.assertRaises(ValueError):
            MockTableColumnType1.from_value("BadValue")

    def test__value_is_compatible_superset_returns_class_unparameterized(
            self):
        self.assertEqual(
            MockTableColumnType2(),
            MockTableColumnType2.from_value("dummy"))

    def test_get_depth_returns_correct_depth_0(self):
        self.assertEqual(MockTableColumnType1().get_depth(), 0)

    def test_get_depth_returns_correct_depth_1(self):
        self.assertEqual(MockTableColumnType3().get_depth(), 1)

    def test_get_depth_returns_correct_depth_gt_1(self):
        self.assertEqual(MockTableColumnType8().get_depth(), 2)

    def test_from_value_non_parameterized_returns_instance(self):
        self.assertEqual(
            MockTableColumnType1.from_value('dummy'),
            MockTableColumnType1(5))


class TestTableColumnMethods(unittest.TestCase):
    """
    Test all methods for the base TableColumn class
    """

    def test_repr(self):
        self.assertEqual(str(schematic.TableColumn(
            "TestColumn1", MockTableColumnType1())), "TestColumn1: MockTableColumnType1 (1)")


class TestTableDefinitionMethods(unittest.TestCase):
    """
    Test all the methods for the base TableDefinition class
    """

    def setUp(self):
        columns = [
            schematic.TableColumn(
                "TestColumn1", MockTableColumnType1()), schematic.TableColumn(
                "TestColumn2", MockTableColumnType2()), schematic.TableColumn(
                "TestColumn3", MockTableColumnType3())]
        self.table_definition = schematic.TableDefinition(
            "TestTableDefinition", columns)

    def test_repr_returns_correct_string(self):
        self.assertEqual(str(self.table_definition),
                         "TestTableDefinition: [TestColumn1: MockTableColumnType1 (1),TestColumn2: MockTableColumnType2,TestColumn3: MockTableColumnType3]")

    def test_eq_returns_false_different_type(self):
        self.assertFalse(
            schematic.TableDefinition(
                "TestTableDefinition",
                []) == MockTableDefinitionClass(
                "TestTableDefinition",
                []))

    def test_eq_returns_true_when_eq(self):
        self.assertTrue(
            MockTableDefinitionClass(
                "TestTableDefinition", [
                    schematic.TableColumn(
                        "Test", MockTableColumnType5())]) == MockTableDefinitionClass(
                "TestTableDefinition", [
                    schematic.TableColumn(
                        "Test", MockTableColumnType5())]))

    def test_eq_returns_false_different_name(self):
        self.assertFalse(
            MockTableDefinitionClass(
                "TestTableDefinitionOther",
                []) == MockTableDefinitionClass(
                "TestTableDefinition",
                []))

    def test_eq_returns_false_different_columns(self):
        self.assertFalse(
            MockTableDefinitionClass(
                "TestTableDefinition", [
                    schematic.TableColumn(
                        "Test", MockTableColumnType5())]) == MockTableDefinitionClass(
                "TestTableDefinition", []))

    def test_create_sql_raises_notimplemented(self):
        with self.assertRaises(NotImplementedError):
            self.table_definition.create_sql()

    def test_create_table_raises_notimplemented(self):
        with self.assertRaises(NotImplementedError):
            self.table_definition.create_table(
                "dummy_arg", dummy_kwarg="dummy_kwarg")

    def test_to_sql_returns_name(self):
        self.assertEqual(
            self.table_definition.to_sql(),
            "TestTableDefinition")

    def test_add_new_column(self):
        new_column = schematic.TableColumn(
            "TestColumn4", MockTableColumnType4())
        self.table_definition.add_column(new_column)
        self.assertIn(new_column, self.table_definition.columns)

    def test_add_existing_column(self):
        with self.assertRaises(ValueError):
            self.table_definition.add_column(self.table_definition.columns[0])

    def test_update_existing_column_updates(self):
        old_column = self.table_definition.columns[0]
        updated_column = schematic.TableColumn(
            "TestColumn1", MockTableColumnType2)
        self.table_definition.update_column(updated_column)
        self.assertIn(updated_column, self.table_definition.columns)
        self.assertNotIn(old_column, self.table_definition.columns)

    def test_update_nonexistent_column(self):
        new_column = schematic.TableColumn("TestColumn9", MockTableColumnType8)
        with self.assertRaises(ValueError):
            self.table_definition.update_column(new_column)

    def test_get_rows_raises_notimplemented(self):
        with self.assertRaises(NotImplementedError):
            self.table_definition.get_rows(
                "dummy_arg", dummy_kwarg="dummy_kwarg")


class TestSchematicMethods(unittest.TestCase):
    """
    Test all the methods for the base Schematic class
    """

    def test_get_type_raises_valueerror_for_base(self):
        with self.assertRaises(ValueError):
            schematic.Schematic().get_type("dummy")

    def test_get_type_raises_valueerror_when_no_match(self):
        with self.assertRaises(ValueError):
            MockSchematic().get_type("BadValue")

    def test_get_type_returns_deepest(self):
        self.assertEqual(MockSchematic().get_type("TestDeepest"),
                         MockTableColumnType5())

    def test_get_type_returns_new_instance_when_compatible_with_previous_type_class(
            self):
        self.assertEqual(
            MockTableColumnType1(10),
            MockSchematic().get_type(
                "tenletters",
                previous_type=MockTableColumnType1(6)))

    def test_get_type_returns_new_class_when_incompatible_with_previous_type_class(
            self):
        self.assertEqual(
            MockTableColumnType1(10),
            MockSchematic().get_type(
                "tenletters",
                previous_type=MockTableColumnType8()))

    def test_get_type_returns_previous_type_when_compatible(self):
        self.assertEqual(
            MockSchematic().get_type(
                "TestDeepest",
                previous_type=MockTableColumnType5()),
            MockTableColumnType5())

    def test_get_type_returns_least_restrictive_when_no_other_match(self):
        self.assertEqual(MockSchematic().get_type("MatchNone"),
                         MockTableColumnType1(9))

    def test_column_types_yields_all(self):
        all_column_types = frozenset([str(MockTableColumnType8()),
                                      str(MockTableColumnType7()),
                                      str(MockTableColumnType6()),
                                      str(MockTableColumnType5()),
                                      str(MockTableColumnType4()),
                                      str(MockTableColumnType3()),
                                      str(MockTableColumnType2()),
                                      str(MockTableColumnType1())])
        yielded_column_types = frozenset(
            [str(t()) for t in MockSchematic().column_types()])
        self.assertEqual(all_column_types, yielded_column_types)

    def test_column_type_from_name_raises_notimplemented_base(self):
        with self.assertRaises(NotImplementedError):
            schematic.Schematic().column_type_from_name("dummy")

    def test_get_distance_from_leaf_node_returns_correct_distance_0(self):
        self.assertEqual(
            MockSchematic().get_distance_from_leaf_node(MockTableColumnType8), 0)

    def test_get_distance_from_leaf_node_returns_correct_distance_1(self):
        self.assertEqual(
            MockSchematic().get_distance_from_leaf_node(MockTableColumnType6), 1)

    def test_get_distance_from_leaf_node_returns_correct_distance_gt_1(self):
        self.assertEqual(
            MockSchematic().get_distance_from_leaf_node(MockTableColumnType5), 2)


class TestTopLevelFunctions(unittest.TestCase):
    def test_get_schematic_by_name(self):
        self.assertEqual(schematic.get_schematic_by_name("redshift"),
                         schematic.schematics.RedshiftSchematic)
        self.assertEqual(schematic.get_schematic_by_name("csv"),
                         schematic.schematics.CSVSchematic)
