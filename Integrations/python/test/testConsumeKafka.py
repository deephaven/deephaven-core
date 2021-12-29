#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

##############################################################################
# NOTE: the jvm should have been initialized, or this test will certainly fail
##############################################################################

import sys
import jpy
import os

from deephaven import ConsumeKafka as ck
from deephaven import Types as dh

if sys.version_info[0] < 3:
    import unittest2 as unittest
    # not part of the standard library, installed via pip (or the like)
    # it provides backward compatibility with python3 style subTest context manager (handy for complex tests)
else:
    import unittest


class TestConsumeKafka(unittest.TestCase):
    """
    Test cases for the deephaven.ConsumeKafka  module (performed locally) -
    """

    def _assertCommonCols(self, cols):
        self.assertEquals("KafkaPartition", cols[0].getName())
        self.assertEquals(dh.int_.clazz(), cols[0].getDataType())
        self.assertEquals("KafkaOffset", cols[1].getName())
        self.assertEquals(dh.long_.clazz(), cols[1].getDataType())
        self.assertEquals("KafkaTimestamp", cols[2].getName())
        self.assertEquals(dh.datetime.clazz(), cols[2].getDataType())

        
    def testBasicConstants(self):
        """
        Check that the basic constants are imported and visible.
        """
        
        self.assertIsNotNone(ck.SEEK_TO_BEGINNING)
        self.assertIsNotNone(ck.DONT_SEEK)
        self.assertIsNotNone(ck.SEEK_TO_END)
        self.assertIsNotNone(ck.FROM_PROPERTIES)
        self.assertIsNotNone(ck.IGNORE)
        self.assertIsNotNone(ck.ALL_PARTITIONS)
        self.assertIsNotNone(ck.ALL_PARTITIONS_SEEK_TO_BEGINNING)
        self.assertIsNotNone(ck.ALL_PARTITIONS_SEEK_TO_END)
        self.assertIsNotNone(ck.ALL_PARTITIONS_DONT_SEEK)


    def testSimple(self):
        """
        Check a simple Kafka subscription creates the right table.
        """
        t = ck.consumeToTable(
            {'bootstrap.servers' : 'redpanda:29092'},
            'orders',
            key = ck.IGNORE,
            value = ck.simple('Price', dh.double))

        cols = t.getDefinition().getColumns()
        self.assertEquals(4, len(cols))
        self._assertCommonCols(cols)
        self.assertEquals("Price", cols[3].getName())
        self.assertEquals(dh.double.clazz(), cols[3].getDataType())


    def testJson(self):
        """
        Check a JSON Kafka subscription creates the right table.
        """

        t = ck.consumeToTable(
            {'bootstrap.servers' : 'redpanda:29092'},
            'orders',
            key = ck.IGNORE,
            value = ck.json(
                [ ('Symbol', dh.string),
                  ('Side', dh.string),
                  ('Price', dh.double),
                  ('Qty', dh.int_),
                  ('Tstamp', dh.datetime) ],
                mapping = {
                    'jsymbol' : 'Symbol',
                    'jside' : 'Side',
                    'jprice' : 'Price',
                    'jqty' : 'Qty',
                    'jts' : 'Tstamp' }
            ),
            table_type = 'append'
        )

        cols = t.getDefinition().getColumns()
        self.assertEquals(8, len(cols))
        self._assertCommonCols(cols)

        self.assertEquals("Symbol", cols[3].getName())
        self.assertEquals(dh.string.clazz(), cols[3].getDataType())
        self.assertEquals("Side", cols[4].getName())
        self.assertEquals(dh.string.clazz(), cols[4].getDataType())
        self.assertEquals("Price", cols[5].getName())
        self.assertEquals(dh.double.clazz(), cols[5].getDataType())
        self.assertEquals("Qty", cols[6].getName())
        self.assertEquals(dh.int_.clazz(), cols[6].getDataType())
        self.assertEquals("Tstamp", cols[7].getName())
        self.assertEquals(dh.datetime.clazz(), cols[7].getDataType())


    def testAvro(self):
        """
        Check an Avro Kafka subscription creates the right table.
        """

        schema = \
        """
        { "type" : "record",
          "namespace" : "io.deephaven.examples",
          "name" : "share_price",
          "fields" : [
            { "name" : "Symbol", "type" : "string" },
            { "name" : "Side",   "type" : "string" },
            { "name" : "Qty",    "type" : "int"    },
            { "name" : "Price",  "type" : "double" }
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
            http://redpanda:8081/subjects/share_price_record/versions
        """ % schema_str

        r = os.system(sys_str)
        self.assertEquals(0, r)
        
        with self.subTest(msg='straight schema, no mapping'):
            t = ck.consumeToTable(
                { 'bootstrap.servers' : 'redpanda:29092',
                  'schema.registry.url' : 'http://redpanda:8081' },
                'share_price',
                key = ck.IGNORE,
                value = ck.avro('share_price_record', schema_version='1'),
                table_type='append'
            )

            cols = t.getDefinition().getColumns()
            self.assertEquals(7, len(cols))
            self._assertCommonCols(cols)

            self.assertEquals("Symbol", cols[3].getName())
            self.assertEquals(dh.string.clazz(), cols[3].getDataType())
            self.assertEquals("Side", cols[4].getName())
            self.assertEquals(dh.string.clazz(), cols[4].getDataType())
            self.assertEquals("Qty", cols[5].getName())
            self.assertEquals(dh.int_.clazz(), cols[5].getDataType())
            self.assertEquals("Price", cols[6].getName())
            self.assertEquals(dh.double.clazz(), cols[6].getDataType())

        with self.subTest(
                msg='mapping_only (filter out some schema fields)'):
            m = {'Symbol' : 'Ticker', 'Price' : 'Dollars'}
            t = ck.consumeToTable(
                { 'bootstrap.servers' : 'redpanda:29092',
                  'schema.registry.url' : 'http://redpanda:8081' },
                'share_price',
                key = ck.IGNORE,
                value = ck.avro('share_price_record', mapping_only=m),
                table_type='append'
            )

            cols = t.getDefinition().getColumns()
            self.assertEquals(5, len(cols))
            self._assertCommonCols(cols)

            self.assertEquals("Ticker", cols[3].getName())
            self.assertEquals(dh.string.clazz(), cols[3].getDataType())
            self.assertEquals("Dollars", cols[4].getName())
            self.assertEquals(dh.double.clazz(), cols[4].getDataType())

        with self.subTest(msg='mapping (rename some fields)'):
            m = {'Symbol' : 'Ticker', 'Qty' : 'Quantity'}
            t = ck.consumeToTable(
                { 'bootstrap.servers' : 'redpanda:29092',
                  'schema.registry.url' : 'http://redpanda:8081' },
                'share_price',
                key = ck.IGNORE,
                value = ck.avro('share_price_record', mapping=m),
                table_type='append'
            )

            cols = t.getDefinition().getColumns()
            self.assertEquals(7, len(cols))
            self._assertCommonCols(cols)

            self.assertEquals("Ticker", cols[3].getName())
            self.assertEquals(dh.string.clazz(), cols[3].getDataType())
            self.assertEquals("Side", cols[4].getName())
            self.assertEquals(dh.string.clazz(), cols[4].getDataType())
            self.assertEquals("Quantity", cols[5].getName())
            self.assertEquals(dh.int_.clazz(), cols[5].getDataType())
            self.assertEquals("Price", cols[6].getName())
            self.assertEquals(dh.double.clazz(), cols[6].getDataType())

