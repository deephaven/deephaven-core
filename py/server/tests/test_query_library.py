#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import query_library
from tests.testbase import BaseTestCase


class QueryLibraryTestCase(BaseTestCase):

    def test_import_class(self):
        self.assertNotIn("import java.util.concurrent.ConcurrentLinkedDeque;", query_library.imports())
        query_library.import_class("java.util.concurrent.ConcurrentLinkedDeque")
        self.assertIn("import java.util.concurrent.ConcurrentLinkedDeque;", query_library.imports())

    def test_import_static(self):
        self.assertNotIn("import static java.util.concurrent.ConcurrentHashMap.*;", query_library.imports())
        query_library.import_static("java.util.concurrent.ConcurrentHashMap")
        self.assertIn("import static java.util.concurrent.ConcurrentHashMap.*;", query_library.imports())

    def test_import_package(self):
        self.assertNotIn("import java.util.concurrent.*;", query_library.imports())
        query_library.import_package("java.util.concurrent")
        self.assertIn("import java.util.concurrent.*;", query_library.imports())


if __name__ == '__main__':
    unittest.main()
