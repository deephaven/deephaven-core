#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from test_helper import start_jvm
start_jvm()

from deephaven.perfmon import query_update_performance, query_performance, query_operation_performance, server_state

class TestClass(unittest.TestCase):
    def test_performance_queries(self):
        q = query_performance(1)
        self.assertTrue(q.to_string())
        q = query_operation_performance(1)
        self.assertTrue(q.to_string())
        q = server_state()
        self.assertTrue(q.to_string())
        q = query_update_performance(1)
        self.assertTrue(q.to_string())

if __name__ == "__main__":
    unittest.main(verbosity=2)
