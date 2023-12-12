#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

import jpy
from deephaven.plugin.js import JsPlugin

_JJsPlugin = jpy.get_type("io.deephaven.plugin.js.JsPlugin")
_JPath = jpy.get_type("java.nio.file.Path")


def to_j_js_plugin(js_plugin: JsPlugin) -> jpy.JType:
    with js_plugin.path() as tmp_path:
        path = tmp_path
    if not path.exists():
        raise NotImplementedError(f"The Deephaven JsPlugin server-side currently only supports normal filesystem resources. {js_plugin}")
    main_path = root_path.relativize(root_path.resolve(js_plugin.main))
    builder = _JJsPlugin.builder()
    builder.name(js_plugin.name)
    builder.version(js_plugin.version)
    builder.main(main_path)
    builder.path(path)
    return builder.build()
