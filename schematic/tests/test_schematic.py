#!/usr/bin/env python
import unittest
import schematic

class TestTableDefinitionMethods(unittest.TestCase):
    """
    Test all the methods for the base TableDefinition class
    """

    def test_from_csv_raises_not_implemented(self):
        self.assertRaises(NotImplementedError, schematic.TableDefinition.from_csv, None)
        

if __name__ == '__main__':
    unittest.main()

