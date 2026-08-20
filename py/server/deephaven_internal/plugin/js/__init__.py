#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import pathlib
from typing import Any

import jpy
from deephaven.plugin.js import JsPlugin

_JJsPlugin = jpy.get_type("io.deephaven.plugin.js.JsPlugin")
_JPath = jpy.get_type("java.nio.file.Path")
_JHashMap = jpy.get_type("java.util.HashMap")
_JArrayList = jpy.get_type("java.util.ArrayList")


def _to_j_object(value: Any) -> Any:
    """Recursively converts Python containers into Java Map / List instances.

    Nested containers must be converted explicitly; otherwise jpy passes them through as opaque
    org.jpy.PyObject values, which the server cannot serialize into the js-plugins manifest.
    """
    if isinstance(value, dict):
        j_map = _JHashMap(len(value))
        for k, v in value.items():
            j_map.put(_to_j_object(k), _to_j_object(v))
        return j_map
    if isinstance(value, (list, tuple, set, frozenset)):
        j_list = _JArrayList(len(value))
        for v in value:
            j_list.add(_to_j_object(v))
        return j_list
    return value


def to_j_js_plugin(js_plugin: JsPlugin) -> jpy.JType:
    path = js_plugin.path()
    if not isinstance(path, pathlib.Path):
        # Adding a little bit of extra safety for this version of the server.
        # There's potential that the return type of JsPlugin.path expands in the future.
        raise Exception(
            f"Expecting pathlib.Path, is type(js_plugin.path())={type(path)}, js_plugin={js_plugin}"
        )
    j_path = _JPath.of(str(path))
    main_path = j_path.relativize(j_path.resolve(js_plugin.main))
    builder = _JJsPlugin.builder()
    builder.name(js_plugin.name)
    builder.version(js_plugin.version)
    builder.main(main_path)
    builder.path(j_path)
    # "loader" is not required field on the deephaven.plugin.js.JsPlugin interface, so we duck-type check for it
    # here to allow plugins to opt-in to providing loader configuration without requiring a new release of the
    # deephaven-plugin package.
    loader = getattr(js_plugin, "loader", None)
    if loader is not None:
        builder.loader(_to_j_object(loader))
    return builder.build()
