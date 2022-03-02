#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

import jpy

from deephaven2.table import Table
from deephaven2.uri import get_deephaven_wrapper_types, resolve, wrap_resolved_object
from tests.testbase import BaseTestCase


class UriTestCase(BaseTestCase):
    def test_list_all_wrapper_classes(self):
        wrapper_cls_set = get_deephaven_wrapper_types()
        self.assertGreater(len(wrapper_cls_set), 0)  # add assertion here

    def test_uri_local_table(self):
        _JResolver = jpy.get_type("io.deephaven.server.uri.CsvTableResolver")
        _JURI = jpy.get_type("java.net.URI")
        j_resolver = _JResolver()
        juri = _JURI.create(
            "csv+https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro"
            ".csv"
        )
        t = wrap_resolved_object(j_resolver.resolve(juri))
        self.assertIsInstance(t, Table)


if __name__ == "__main__":
    unittest.main()
