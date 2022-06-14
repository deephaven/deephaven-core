#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

import jpy

from deephaven.jcompat import j_hashset
from deephaven.table import Table
from deephaven.uri import resolve
from tests.testbase import BaseTestCase


class UriTestCase(BaseTestCase):
    def test_uri_local_table(self):
        _JResolver = jpy.get_type("io.deephaven.server.uri.CsvTableResolver")
        _JUriResolvers = jpy.get_type("io.deephaven.uri.resolver.UriResolvers")
        _JUriResolversInstance = jpy.get_type("io.deephaven.uri.resolver.UriResolversInstance")
        j_resolver = _JResolver()
        j_resolver_set = j_hashset({j_resolver})
        j_resolvers = _JUriResolvers(j_resolver_set)
        _JUriResolversInstance.init(j_resolvers)

        uri = "csv+https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro.csv"
        t = resolve(uri)
        self.assertIsInstance(t, Table)


if __name__ == "__main__":
    unittest.main()
