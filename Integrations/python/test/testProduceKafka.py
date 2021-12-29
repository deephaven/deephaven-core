#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

##############################################################################
# NOTE: the jvm should have been initialized, or this test will certainly fail
##############################################################################

import sys
import jpy
import os

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
        cleanup = pk.produceFromTable(
            t,
            {'bootstrap.servers' : 'redpanda:29092'},
            'orders',
            key = pk.IGNORE,
            value = pk.simple('Price')
        )

        self.assertIsNotNone(cleanup)
        cleanup()

    def tableHelper(self):
        t = dh.table_of( {
            'Symbol' : (dh.string, [ 'MSFT', 'GOOG', 'AAPL', 'AAPL' ]),
            'Side'   : (dh.string, [ 'B', 'B', 'S', 'B' ]),
            'Qty'    : (dh.int_,   [ 200, 100, 300, 50 ]),
            'Price'  : (dh.double, [ 210.0, 310.5, 411.0, 411.5 ])
        } )
        return t
        
    def testJsonOnlyColumns(self):
        t = self.tableHelper()
        cleanup = pk.produceFromTable(
            t,
            {'bootstrap.servers' : 'redpanda:29092'},
            'orders',
            key = pk.IGNORE,
            value = pk.json(['Symbol', 'Price']),
            last_by_key_columns = False
        )
        
        self.assertIsNotNone(cleanup)
        cleanup()
            
    def testJsonAllArguments(self):
        t = self.tableHelper()
        cleanup = pk.produceFromTable(
            t,
            {'bootstrap.servers' : 'redpanda:29092'},
            'orders',
            key = pk.IGNORE,
            value = pk.json(
                ['Symbol', 'Price'],
                mapping={ 'Symbol' : 'jSymbol', 'Price' : 'jPrice' },
                timestamp_field='jTs'
            ),
            last_by_key_columns = False
        )
        
        self.assertIsNotNone(cleanup)
        cleanup()

    def testAvro(self):
        schema = \
        """
        { "type" : "record",
          "namespace" : "io.deephaven.examples",
          "name" : "share_price_timestamped",
          "fields" : [
            { "name" : "Symbol", "type" : "string" },
            { "name" : "Side",   "type" : "string" },
            { "name" : "Qty",    "type" : "int"    },
            { "name" : "Price",  "type" : "double" },
            { "name" : "Timestamp",
              "type" : {
                 "type" : "long",
                 "logicalType" : "timestamp-micros"
              }
            }
          ]
        }
        """

        schema_str = '{ "schema" : "%s" }' % \
            schema.replace('\n', ' ').replace('"', '\\"')

        sys_str = \
        """
        curl -X POST \
            -H 'Content-type: application/vnd.schemaregistry.v1+json; artifactType=AVRO' \
            --data-binary '%s' \
            http://redpanda:8081/subjects/share_price_timestamped_record/versions
        """ % schema_str

        r = os.system(sys_str)
        self.assertEquals(0, r)

        t = self.tableHelper()
        cleanup = pk.produceFromTable(
            t,
            { 'bootstrap.servers' : 'redpanda:29092',
              'schema.registry.url' : 'http://redpanda:8081' },
            'share_price_timestamped',
            key = pk.IGNORE,
            value = pk.avro(
                'share_price_timestamped_record',
                timestamp_field='Timestamp'
            ),
            last_by_key_columns = False
        )
        
        self.assertIsNotNone(cleanup)
        cleanup()
