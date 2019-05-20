import schematic
import unittest
from click.testing import CliRunner

class TestCLICommands(unittest.TestCase):

    def setUp(self):
        self.runner = CliRunner()

    def test_top_level_runs(self):
        result = self.runner.invoke(schematic.cli, [])
        self.assertEqual(result.exit_code, 0)
