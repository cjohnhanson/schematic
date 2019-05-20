import unittest
import schematic


class TestTableColumnType(unittest.TestCase):
    """
    Test all the methods for the base TableColumnType class
    """

    def setUp(self):
        """
        Instantiate a restrictivity tree
        """
        # TODO
        pass

    def test_next_less_restrictive_tree_with_cycles_raises_error(self):
        """
        Instantiation which would create a cycle should
        raise NextLessRestrictiveCycleError
        """
        # TODO
        pass

    def test_lt_returns_true_when_less_restrictive(self):
        """
        """
        # TODO
        pass

    def test_lt_returns_false_when_false_when_more_restrictive(self):
        """
        """
        # TODO
        pass

    def test_lt_returns_false_when_false_when_equally_restrictive(self):
        """
        """
        # TODO
        pass

    def test_gt_returns_true_when_more_restrictive(self):
        """
        """
        # TODO
        pass

    def test_gt_returns_false_when_less_restrictive(self):
        """
        """
        # TODO
        pass

    def test_gt_returns_false_when_equally_restrictive(self):
        """
        """
        # TODO
        pass


class TestTableColumnMethods(unittest.TestCase):
    """
    Test all methods for the base TableColumn class
    """
    pass


class TestTableDefinitionMethods(unittest.TestCase):
    """
    Test all the methods for the base TableDefinition class
    """

    def test_create_sql(self):
        # TODO
        pass

    def test_identifier_string(self):
        # TODO
        pass

    def test_add_new_column(self):
        # TODO
        pass

    def test_add_existing_column(self):
        # TODO
        pass

    def test_update_existing_column(self):
        # TODO
        pass

    def test_update_nonexistent_column(self):
        # TODO
        pass


class TestSchematic(unittest.TestCase):
    """
    Test all the methods for the base Schematic class
    """

    def test_get_type_not_implemented(self):
        # TODO
        pass

    def test_column_types_yields_all(self):
        # TODO
        pass
