#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

import unittest

from tests.test_console import ConsoleTestCase
from tests.test_query import QueryTestCase
from tests.test_session import SessionTestCase
from tests.test_table import TableTestCase

if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(SessionTestCase))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(QueryTestCase))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TableTestCase))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(ConsoleTestCase))

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
