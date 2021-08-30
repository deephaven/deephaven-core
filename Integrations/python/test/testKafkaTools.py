#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

##############################################################################
# NOTE: the jvm should have been initialized, or this test will certainly fail
##############################################################################

import sys
import jpy

from deephaven import KafkaTools as kt
from deephaven import Types as dh

if sys.version_info[0] < 3:
    import unittest2 as unittest
    # not part of the standard library, installed via pip (or the like)
    # it provides backward compatibility with python3 style subTest context manager (handy for complex tests)
else:
    import unittest


class TestTableTools(unittest.TestCase):
    """
    Test cases for the deephaven.KafkaTools module (performed locally) -
    """

    @classmethod
    def setUpClass(self):
        kt._defineSymbols()

    def testBasicConstants(self):
        """
        Check that the basic constants are imported and visible.
        """
        
        self.assertIsNotNone(kt.SEEK_TO_BEGINNING)
        self.assertIsNotNone(kt.DONT_SEEK)
        self.assertIsNotNone(kt.SEEK_TO_END)
        self.assertIsNotNone(kt.FROM_PROPERTIES)
        self.assertIsNotNone(kt.IGNORE)
        self.assertIsNotNone(kt.ALL_PARTITIONS)
        self.assertIsNotNone(kt.ALL_PARTITIONS_SEEK_TO_BEGINNING)
        self.assertIsNotNone(kt.ALL_PARTITIONS_SEEK_TO_END)
        self.assertIsNotNone(kt.ALL_PARTITIONS_DONT_SEEK)


    def testSimple(self):
        """
        Check a simple Kafka subscription creates the right table.
        """
        t = kt.consumeToTable(
            {'bootstrap.servers' : 'redpanda:29092'},
            'orders',
            key = kt.IGNORE,
            value = kt.simple('Price', dh.double))
        self.assertIsNotNone(t)
