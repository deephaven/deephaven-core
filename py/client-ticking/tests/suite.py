#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from tests.test_ticking_basic import TickingBasicTestCase

if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TickingBasicTestCase))

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
