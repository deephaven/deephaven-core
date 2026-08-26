#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import json
import pathlib
import tempfile
import unittest

import jpy
from deephaven.plugin.js import JsPlugin

from deephaven_internal.plugin.js import to_j_js_plugin
from tests.testbase import BaseTestCase

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
