import schematic
import unittest


class MockSqlMixinClass(schematic.NameSqlMixin):
    name = "MockSqlMixinClass"


class TestNameSqlMixinMethods(unittest.TestCase):

    def test_to_sql(self):
        self.assertEqual(MockSqlMixinClass().to_sql(), "MockSqlMixinClass")


class MockDictableClass(schematic.DictableMixin, object):
    def __init__(self, a, b):
        self.a = a
        self.b = b


class TestDictableMixinMethods(unittest.TestCase):

    def test_to_dict(self):
        self.assertEqual({'a': 1, 'b': 2},
                         MockDictableClass(1, 2).to_dict())

    def test_from_dict(self):
        test_mock_dictable_class = MockDictableClass(1, 2)
        self.assertEqual(test_mock_dictable_class,
                         MockDictableClass.from_dict({'a': 1, 'b': 2}))

    def test_eq_returns_false_when_other_not_dictable(self):
        self.assertFalse(MockDictableClass(1, 2) == 1)
