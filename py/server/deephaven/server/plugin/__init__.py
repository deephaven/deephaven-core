#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

from deephaven_internal._jvm import jpy

_JCallbackAdapter = jpy.get_type('io.deephaven.server.plugin.python.CallbackAdapter')

def initialize_all_and_register_into(callback: _JCallbackAdapter):
    try:
        from . import register
    except ModuleNotFoundError as e:
        # deephaven.plugin is an optional dependency, so if it can't be found, there are no Deephaven python plugins
        # to register
        if e.name == 'deephaven.plugin':
            return
        raise e
    register.initialize_all_and_register_into(callback)
