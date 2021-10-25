#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

##############################################################################
# NOTE: the jvm should have been initialized, or this test will certainly fail
##############################################################################

import sys
import jpy

from deephaven import ProduceKafka as pk
from deephaven import Types as dh

if sys.version_info[0] < 3:
    import unittest2 as unittest
    # not part of the standard library, installed via pip (or the like)
    # it provides backward compatibility with python3 style subTest context manager (handy for complex tests)
else:
    import unittest


class TestProduceKafka(unittest.TestCase):
    """
    Test cases for the deephaven.ConsumeKafka  module (performed locally) -
    """

    def testBasicConstants(self):
        """
        Check that the basic constants are imported and visible.
        """
        
        self.assertIsNotNone(pk.IGNORE)


    def testSimple(self):
        
        """
        Check a simple Kafka subscription creates the right table.
        """
        t = dh.table_of( { 'Price' : (dh.double, [ 10.0, 10.5, 11.0, 11.5 ]) } )
        c = pk.produceFromTable(
            t,
            {'bootstrap.servers' : 'redpanda:29092'},
            'orders',
            key = pk.IGNORE,
            value = pk.simple('Price')
        )

        self.assertIsNotNone(c)
        c.call()
