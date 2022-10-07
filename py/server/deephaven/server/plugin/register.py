#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import jpy
import deephaven.plugin

from typing import Union, Type
from deephaven.plugin import Plugin, Registration
from deephaven.plugin.object import ObjectType
from deephaven.plugin.js import JsPlugin
from .object import ObjectTypeAdapter

_JCallbackAdapter = jpy.get_type('io.deephaven.server.plugin.python.CallbackAdapter')


def initialize_all_and_register_into(callback: _JCallbackAdapter):
    deephaven.plugin.register_all_into(RegistrationAdapter(callback))


class RegistrationAdapter(Registration.Callback):
    def __init__(self, callback: _JCallbackAdapter):
        self._callback = callback

    def register(self, plugin: Union[Plugin, Type[Plugin]]):
        if isinstance(plugin, type):
            # If registering a class, instantiate it before adapting it and passing to java
            plugin = plugin()
        if isinstance(plugin, ObjectType):
            self._callback.registerObjectTypePlugin(plugin.name, ObjectTypeAdapter(plugin))
        elif isinstance(plugin, JsPlugin):
            with plugin.distribution_path() as distribution_path:
                self._callback.registerJsPlugin(str(distribution_path), plugin.name, plugin.version, plugin.main)
        else:
            raise NotImplementedError

    def __str__(self):
        return str(self._callback)
