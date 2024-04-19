#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest

import jpy

from deephaven.jcompat import j_hashset
from deephaven.remote import barrage_session

from tests.testbase import BaseTestCase


class RemoteTestCase(BaseTestCase):
    def test_session(self):
        _JResolver = jpy.get_type("io.deephaven.server.uri.BarrageTableResolver")
        _JUriResolvers = jpy.get_type("io.deephaven.uri.resolver.UriResolvers")
        _JUriResolversInstance = jpy.get_type("io.deephaven.uri.resolver.UriResolversInstance")
        j_resolver = _JResolver.get()
        j_resolver_set = j_hashset({j_resolver})
        j_resolvers = _JUriResolvers(j_resolver_set)
        _JUriResolversInstance.init(j_resolvers)

        session = barrage_session(host="core-server-2-1", port=10000, auth_type="Anonymous")
        t = session.fetch(b'hgd\xc6\xf3\xea\xd3\x15\xbabB#\x1e-\x94\xfcI')
        self.assertEqual(t.size, 1000)
        self.assertEqual(len(t.columns), 2)
        sp = t.snapshot()
        self.assertEqual(sp.size, 1000)
        t1 = t.update("Z = X + Y")
        self.assertEqual(t1.size, 1000)


if __name__ == "__main__":
    unittest.main()
