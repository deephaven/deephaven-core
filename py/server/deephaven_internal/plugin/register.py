#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#

from typing import TYPE_CHECKING, Union, cast

import deephaven.plugin
import jpy
from deephaven.plugin import Callback, Plugin
from deephaven.plugin.js import JsPlugin
from deephaven.plugin.object_type import ObjectType

from .js import to_j_js_plugin
from .object import ObjectTypeAdapter

if TYPE_CHECKING:
    from typing_extensions import TypeAlias  # novermin  # noqa

_JCallbackAdapter = cast(
    type, jpy.get_type("io.deephaven.server.plugin.python.CallbackAdapter")
)  # type: TypeAlias


def initialize_all_and_register_into(callback: _JCallbackAdapter):
    """Python method that Java can call to create plugin instances on startup."""
    deephaven.plugin.register_all_into(RegistrationAdapter(callback))


class RegistrationAdapter(Callback):
    """Python implementation of Callback that delegates to its Java counterpart."""

    def __init__(self, callback: _JCallbackAdapter):
        self._callback = callback

    def register(self, plugin: Union[Plugin, type[Plugin]]):
        if isinstance(plugin, type):
            # If registering a class, instantiate it before adapting it and passing to java
            plugin = plugin()
        if isinstance(plugin, ObjectType):
            self._callback.registerObjectType(plugin.name, ObjectTypeAdapter(plugin))
        elif isinstance(plugin, JsPlugin):
            self._callback.registerJsPlugin(to_j_js_plugin(plugin))
        else:
            raise NotImplementedError(f"Unexpected type: {type(plugin)}")

    def __str__(self):
        return str(self._callback)
