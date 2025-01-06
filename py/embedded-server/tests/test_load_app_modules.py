#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest
import importlib
import pkgutil
import sys


class LoadAllModules(unittest.TestCase):
    def test_import_all_packages(self):
        # First start the server that is defined in this package
        from deephaven_server import Server
        Server().start()

        # Then iterate through all deephaven packages to make sure they all can be imported
        # using the wheel that we distribute
        pkg = importlib.import_module('deephaven')

        mods = pkgutil.walk_packages(pkg.__path__, prefix='deephaven.')
        for mod in mods:
            if mod.name not in sys.modules:
                try:
                    importlib.import_module(mod.name)
                except:
                    ...
