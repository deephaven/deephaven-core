#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import unittest

from deephaven2 import empty_table

from deephaven2.perfmon import process_info_log, process_metrics_log, process_memory_log, \
    query_operation_performance_log, query_performance_log, update_performance_log, metrics_get_counters, \
    metrics_reset_counters
from tests.testbase import BaseTestCase


def create_some_counters():
    t = empty_table(10).update(formulas=["X=i"])
    t2 = empty_table(10).update(formulas=["X=i"])
    return t.join(t2, on=["X"])


class PerfmonTestCase(BaseTestCase):

    def test_metrics_get_counters(self):
        metrics_reset_counters()
        t = create_some_counters()
        # counters should accumulate some values after performing some operations
        counters_str = metrics_get_counters()
        t = create_some_counters()
        # counters now should have different values after performing the same operation one more time
        counters_str2 = metrics_get_counters()
        self.assertNotEqual(counters_str, counters_str2)

        # after reset and performing the same operation again, the counters' values should be the same as the 1st one
        metrics_reset_counters()
        t = create_some_counters()
        counters_str3 = metrics_get_counters()
        self.assertEqual(counters_str, counters_str3)

    def test_process_logs(self):
        log_table = process_info_log()
        self.assertTrue(log_table.to_string())
        log_table = process_memory_log()
        self.assertTrue(log_table.to_string())
        log_table = process_metrics_log()
        self.assertTrue(log_table.to_string())

    def test_query_logs(self):
        log_table = query_operation_performance_log()
        self.assertTrue(log_table.to_string())
        log_table = query_performance_log()
        self.assertTrue(log_table.to_string())
        log_table = update_performance_log()
        self.assertTrue(log_table.to_string())


if __name__ == '__main__':
    unittest.main()
