import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO

from dsline.__main__ import main
from dsline._info import format_info, get_info
from dsline._shm import cleanup, dir_path, inspect, list_regions, region_path, remove


class InfoTests(unittest.TestCase):
    def test_info_contains_backend_status(self) -> None:
        info = get_info()

        self.assertEqual(info["name"], "dsline")
        self.assertEqual(info["channel_backend"], "shm")
        self.assertEqual(info["zero_copy_alloc_publish"], "unavailable")
        self.assertIn("memory", info["storage_backends"])
        self.assertIn("file", info["storage_backends"])
        self.assertIn("mmap", info["storage_backends"])
        self.assertIn("DROP_NEWEST", info["backpressure_strategies"])
        self.assertIn("DROP_OLDEST", info["backpressure_strategies"])

    def test_info_json_format_is_parseable(self) -> None:
        info = get_info()
        rendered = format_info(info, json_output=True)

        self.assertEqual(json.loads(rendered), info)

    def test_info_command_runs(self) -> None:
        stdout = StringIO()

        with redirect_stdout(stdout):
            self.assertEqual(main(["info", "--json"]), 0)

        self.assertEqual(json.loads(stdout.getvalue()), get_info())


class ShmCliTests(unittest.TestCase):
    """Region management over a real (or mocked) runtime directory."""

    def setUp(self) -> None:
        # Point the runtime dir at a temp location so the tests never touch
        # the real %LOCALAPPDATA% region store.
        self._tmp = tempfile.TemporaryDirectory()
        self._env = os.environ.copy()
        if os.name == "nt":
            self._env["LOCALAPPDATA"] = self._tmp.name
        else:
            self._env["XDG_RUNTIME_DIR"] = self._tmp.name
        os.environ.update(self._env)

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._env)
        self._tmp.cleanup()

    def test_region_path_matches_resolved_channel_path(self) -> None:
        import dsline

        name = "cli-name-match"
        with dsline.ShmChannel(name, capacity=2, slot_size=16) as ch:
            self.assertEqual(region_path(name), ch.path)

    def test_list_and_inspect_roundtrip(self) -> None:
        name = "cli-list-inspect"
        path = region_path(name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(b"region" * 10)

        entries = json.loads(list_regions(json_output=True))
        self.assertEqual([e["file"] for e in entries], [os.path.basename(path)])
        self.assertEqual(entries[0]["bytes"], 60)

        info = json.loads(inspect(name, json_output=True))
        self.assertTrue(info["exists"])
        self.assertEqual(info["path"], path)

        stdout = StringIO()
        with redirect_stdout(stdout):
            self.assertEqual(main(["shm", "list", "--json"]), 0)
        self.assertIn(os.path.basename(path), stdout.getvalue())

    def test_inspect_missing_channel(self) -> None:
        info = json.loads(inspect("cli-missing", json_output=True))
        self.assertFalse(info["exists"])

    def test_cleanup_removes_only_old_regions(self) -> None:
        fresh = region_path("cli-fresh")
        stale = region_path("cli-stale")
        os.makedirs(os.path.dirname(fresh), exist_ok=True)
        with open(fresh, "wb") as fh:
            fh.write(b"fresh")
        with open(stale, "wb") as fh:
            fh.write(b"stale")
        old = os.path.getmtime(stale) - 7200
        os.utime(stale, (old, old))

        with redirect_stdout(StringIO()):
            self.assertEqual(main(["shm", "cleanup", "--older-than", "1h"]), 0)
        self.assertTrue(os.path.exists(fresh))
        self.assertFalse(os.path.exists(stale))

    def test_cleanup_rejects_bad_duration(self) -> None:
        with self.assertRaises(SystemExit):
            main(["shm", "cleanup", "--older-than", "soon"])

    def test_remove_deletes_region_file(self) -> None:
        name = "cli-remove"
        path = region_path(name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(b"data")

        removed = json.loads(remove(name, json_output=True))
        self.assertTrue(removed["removed"])
        self.assertFalse(os.path.exists(path))

    def test_dir_command_prints_runtime_dir(self) -> None:
        self.assertEqual(dir_path(False), os.path.join(self._tmp.name, "dsline", "shm"))


if __name__ == "__main__":
    unittest.main()
