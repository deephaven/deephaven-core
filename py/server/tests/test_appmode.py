#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import unittest
from unittest.mock import MagicMock

from deephaven import DHError
from deephaven.appmode import ApplicationState, get_app_state
from tests.testbase import BaseTestCase


class AppModeTestCase(BaseTestCase):
    def _app_state(self, existing_fields):
        """Returns an ApplicationState over a stand-in for the Java object. Like the Java ApplicationState,
        removeField returns nothing and does nothing for an unknown name, and getField returns null for one."""
        j_app_state = MagicMock()
        j_app_state.getField.side_effect = lambda name: existing_fields.get(name)

        def remove_field(name):
            existing_fields.pop(name, None)

        j_app_state.removeField.side_effect = remove_field
        return ApplicationState(j_app_state), j_app_state

    def test_del_existing_field(self):
        fields = {"hello": MagicMock()}
        app, j_app_state = self._app_state(fields)
        del app["hello"]
        j_app_state.removeField.assert_called_once_with("hello")
        self.assertNotIn("hello", fields)

    def test_del_missing_field(self):
        app, j_app_state = self._app_state({})
        with self.assertRaises(KeyError):
            del app["missing"]
        j_app_state.removeField.assert_not_called()

    def test_get_missing_field(self):
        app, _ = self._app_state({})
        with self.assertRaises(KeyError):
            app["missing"]

    def test_get_app_state_outside_startup(self):
        with self.assertRaises(DHError):
            get_app_state()


if __name__ == "__main__":
    unittest.main()
