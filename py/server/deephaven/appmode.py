#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module supports writing Deephaven application mode Python scripts. """
from typing import Dict

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper, pythonify, javaify

_JApplicationContext = jpy.get_type("io.deephaven.appmode.ApplicationContext")
_JApplicationState = jpy.get_type("io.deephaven.appmode.ApplicationState")


class ApplicationState(JObjectWrapper):
    """ The ApplicationState represents the state of an application. """
    j_object_type = _JApplicationState

    @property
    def j_object(self) -> jpy.JType:
        return self.j_app_state

    def __init__(self, j_app_state):
        self.j_app_state = j_app_state

    def __repr__(self):
        return f"id: {self.j_app_state.id()}, name: {self.j_app_state.name()}"

    def __str__(self):
        return repr(self)

    def __getitem__(self, item):
        item = str(item)
        j_field = self.j_app_state.getField(item)
        if not j_field:
            raise KeyError(item)
        return pythonify(j_field.value())

    def __setitem__(self, key, value):
        key = str(key)
        self.j_app_state.setField(key, javaify(value))

    def __delitem__(self, key):
        key = str(key)
        value = self.j_app_state.removeField(key)
        if not value:
            raise KeyError(key)

    @property
    def fields(self) -> Dict[str, object]:
        fields = {}
        j_fields = self.j_app_state.listFields()
        for i in range(j_fields.size()):
            j_field = j_fields.get(i)
            fields[j_field.name()] = pythonify(j_field.value())

        return fields


def get_app_state():
    """ Get the current application state object.

    Raises:
         DHError
    """
    try:
        return ApplicationState(j_app_state=_JApplicationContext.get())
    except Exception as e:
        raise DHError(e, "failed to get the application state.") from e
