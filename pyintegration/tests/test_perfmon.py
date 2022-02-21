#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import unittest

from deephaven2.perfmon import process_info_log, process_metrics_log, process_memory_log, \
    query_operation_performance_log, query_performance_log, update_performance_log
from tests.testbase import BaseTestCase


class PerfmonTestCase(BaseTestCase):
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
