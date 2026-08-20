#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import json
import pathlib
import tempfile
import unittest

import jpy
from deephaven.plugin.js import JsPlugin

from deephaven_internal.plugin.js import _to_j_object, to_j_js_plugin
from tests.testbase import BaseTestCase

_JMap = jpy.get_type("java.util.Map")
_JList = jpy.get_type("java.util.List")
_JObjectMapper = jpy.get_type("com.fasterxml.jackson.databind.ObjectMapper")


class _MyJsPlugin(JsPlugin):
    """A minimal JsPlugin for testing."""

    def __init__(
        self,
        path: pathlib.Path,
        name: str = "@deephaven_test/example",
        version: str = "0.1.0",
        main: str = "dist/index.js",
        loader=None,
    ):
        self._path = path
        self._name = name
        self._version = version
        self._main = main
        self._loader = loader

    def path(self) -> pathlib.Path:
        return self._path

    @property
    def name(self) -> str:
        return self._name

    @property
    def version(self) -> str:
        return self._version

    @property
    def main(self) -> str:
        return self._main

    @property
    def loader(self):
        return self._loader


class _MyLoaderlessJsPlugin:
    """A JsPlugin-like object that does not have a "loader" attribute at all, as is the case for plugins written
    against older versions of the deephaven-plugin package."""

    def __init__(self, path: pathlib.Path):
        self._path = path

    def path(self) -> pathlib.Path:
        return self._path

    name = "@deephaven_test/loaderless"
    version = "0.2.0"
    main = "dist/index.js"


class PluginJsTestCase(BaseTestCase):
    def setUp(self) -> None:
        super().setUp()
        self._tmp_dir = tempfile.TemporaryDirectory()
        self.plugin_path = pathlib.Path(self._tmp_dir.name)
        (self.plugin_path / "dist").mkdir()
        (self.plugin_path / "dist" / "index.js").write_text("// test js plugin")

    def tearDown(self) -> None:
        self._tmp_dir.cleanup()
        super().tearDown()

    def assert_json_equals(self, j_object, expected) -> None:
        """Serializes a Java object with Jackson, the same way the server serializes the loader configuration into
        "js-plugins/manifest.json", and asserts the resulting JSON matches expected."""
        actual = json.loads(_JObjectMapper().writeValueAsString(j_object))
        self.assertEqual(actual, expected)

    def assert_round_trip(self, value) -> None:
        """Asserts that value survives conversion to Java and JSON serialization unchanged."""
        self.assert_json_equals(_to_j_object(value), value)

    def test_basic_values_pass_through(self):
        for value in [None, True, False, 0, 42, -7, 0.0, 1.5, -2.25, "", "a string"]:
            with self.subTest(value=value):
                self.assertIs(_to_j_object(value), value)

    def test_dict(self):
        j_map = _to_j_object({"key": "value", "count": 3})
        self.assertTrue(_JMap.jclass.isInstance(j_map))
        self.assertEqual(j_map.size(), 2)
        self.assertEqual(j_map.get("key"), "value")
        self.assertEqual(j_map.get("count"), 3)

    def test_dict_of_basic_values(self):
        self.assert_round_trip(
            {
                "none": None,
                "true": True,
                "false": False,
                "int": 42,
                "negative_int": -7,
                "float": 1.5,
                "str": "a string",
            }
        )

    def test_nested_dict(self):
        j_map = _to_j_object({"outer": {"inner": {"deepest": "value"}}})
        j_outer = j_map.get("outer")
        self.assertTrue(_JMap.jclass.isInstance(j_outer))
        j_inner = j_outer.get("inner")
        self.assertTrue(_JMap.jclass.isInstance(j_inner))
        self.assertEqual(j_inner.get("deepest"), "value")

    def test_list(self):
        j_list = _to_j_object(["a", "b", "c"])
        self.assertTrue(_JList.jclass.isInstance(j_list))
        self.assertEqual(j_list.size(), 3)
        self.assertEqual(j_list.get(0), "a")
        self.assertEqual(j_list.get(2), "c")

    def test_tuple(self):
        j_list = _to_j_object(("a", "b"))
        self.assertTrue(_JList.jclass.isInstance(j_list))
        self.assert_json_equals(j_list, ["a", "b"])

    def test_set(self):
        j_list = _to_j_object({"a", "b", "c"})
        self.assertTrue(_JList.jclass.isInstance(j_list))
        self.assertEqual(
            sorted(json.loads(_JObjectMapper().writeValueAsString(j_list))),
            ["a", "b", "c"],
        )

    def test_frozenset(self):
        j_list = _to_j_object(frozenset(["a"]))
        self.assertTrue(_JList.jclass.isInstance(j_list))
        self.assert_json_equals(j_list, ["a"])

    def test_empty_containers(self):
        self.assert_round_trip({})
        self.assert_round_trip([])
        self.assert_round_trip({"empty_map": {}, "empty_list": []})

    def test_nested_containers(self):
        self.assert_round_trip(
            {
                "list_of_dicts": [{"a": 1}, {"b": [2, 3]}],
                "dict_of_lists": {"x": [[1, 2], []], "y": [{"z": None}]},
                "deep": {"a": {"b": {"c": {"d": ["e", {"f": True}]}}}},
            }
        )

    def test_list_of_lists(self):
        j_list = _to_j_object([[1, 2], [3]])
        self.assertTrue(_JList.jclass.isInstance(j_list.get(0)))
        self.assert_json_equals(j_list, [[1, 2], [3]])

    def test_realistic_loader(self):
        self.assert_round_trip(
            {
                "name": "@deephaven_test/example-loader",
                "version": "0.1.0",
                "entry": "./dist/loader.js",
                "config": {
                    "enabled": True,
                    "timeout": 30,
                    "ratio": 0.5,
                    "tags": ["one", "two"],
                    "nested": {"deps": ["@deephaven/jsapi-bootstrap"], "extra": None},
                },
            }
        )

    def test_non_string_keys(self):
        # Jackson serializes non-string map keys as strings, which is the same behavior as json.dumps
        self.assert_json_equals(
            _to_j_object({1: "a", 2.5: "b"}), {"1": "a", "2.5": "b"}
        )

    def test_to_j_js_plugin(self):
        j_plugin = to_j_js_plugin(_MyJsPlugin(self.plugin_path))
        self.assertEqual(j_plugin.name(), "@deephaven_test/example")
        self.assertEqual(j_plugin.version(), "0.1.0")
        self.assertEqual(j_plugin.main().toString(), "dist/index.js")
        self.assertEqual(j_plugin.path().toString(), str(self.plugin_path))
        self.assertFalse(j_plugin.loader().isPresent())

    def test_to_j_js_plugin_with_loader(self):
        loader = {"entry": "./dist/loader.js", "config": {"deps": ["a", "b"]}}
        j_plugin = to_j_js_plugin(_MyJsPlugin(self.plugin_path, loader=loader))
        self.assertTrue(j_plugin.loader().isPresent())
        self.assert_json_equals(j_plugin.loader().get(), loader)

    def test_to_j_js_plugin_with_basic_loader(self):
        for loader in [True, 42, 1.5, "a string", ["a", "b"]]:
            with self.subTest(loader=loader):
                j_plugin = to_j_js_plugin(_MyJsPlugin(self.plugin_path, loader=loader))
                self.assertTrue(j_plugin.loader().isPresent())
                self.assert_json_equals(j_plugin.loader().get(), loader)

    def test_to_j_js_plugin_without_loader_attribute(self):
        j_plugin = to_j_js_plugin(_MyLoaderlessJsPlugin(self.plugin_path))
        self.assertEqual(j_plugin.name(), "@deephaven_test/loaderless")
        self.assertFalse(j_plugin.loader().isPresent())

    def test_to_j_js_plugin_non_path(self):
        js_plugin = _MyJsPlugin(self.plugin_path)
        js_plugin._path = str(self.plugin_path)
        with self.assertRaises(Exception) as cm:
            to_j_js_plugin(js_plugin)
        self.assertIn("Expecting pathlib.Path", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
