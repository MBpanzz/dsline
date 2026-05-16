import json
import unittest
from contextlib import redirect_stdout
from io import StringIO

from dsline.__main__ import main
from dsline._info import format_info, get_info


class InfoTests(unittest.TestCase):
    def test_info_contains_prototype_status(self) -> None:
        info = get_info()

        self.assertEqual(info["name"], "dsline")
        self.assertEqual(info["channel_backend"], "inprocess-prototype")
        self.assertEqual(info["zero_copy_alloc_publish"], "unavailable")
        self.assertIn("memory", info["storage_backends"])
        self.assertIn("file", info["storage_backends"])

    def test_info_json_format_is_parseable(self) -> None:
        info = get_info()
        rendered = format_info(info, json_output=True)

        self.assertEqual(json.loads(rendered), info)

    def test_info_command_runs(self) -> None:
        stdout = StringIO()

        with redirect_stdout(stdout):
            self.assertEqual(main(["info", "--json"]), 0)

        self.assertEqual(json.loads(stdout.getvalue()), get_info())


if __name__ == "__main__":
    unittest.main()
