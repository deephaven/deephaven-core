#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

##############################################################################
# NOTE: the jvm should have been initialized, or this test will certainly fail
##############################################################################

import sys
import jpy
import os

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

        cols = t.getDefinition().getColumns()
        self.assertEquals(4, len(cols))
        self._assertCommonCols(cols)
        self.assertEquals("Price", cols[3].getName())
        self.assertEquals(dh.double.clazz(), cols[3].getDataType())


    def testJson(self):
        """
        Check a JSON Kafka subscription creates the right table.
        """

        t = kt.consumeToTable(
            {'bootstrap.servers' : 'redpanda:29092'},
            'orders',
            key = kt.IGNORE,
            value = kt.json(
                [ ('Symbol', dh.string),
                  ('Side', dh.string),
                  ('Price', dh.double),
                  ('Qty', dh.int_) ],
                mapping = {
                    'jsymbol' : 'Symbol',
                    'jside' : 'Side',
                    'jprice' : 'Price',
                    'jqty' : 'Qty'}
            ),
            table_type = 'append'
        )

        cols = t.getDefinition().getColumns()
        self.assertEquals(7, len(cols))
        self._assertCommonCols(cols)

        self.assertEquals("Symbol", cols[3].getName())
        self.assertEquals(dh.string.clazz(), cols[3].getDataType())
        self.assertEquals("Side", cols[4].getName())
        self.assertEquals(dh.string.clazz(), cols[4].getDataType())
        self.assertEquals("Price", cols[5].getName())
        self.assertEquals(dh.double.clazz(), cols[5].getDataType())
        self.assertEquals("Qty", cols[6].getName())
        self.assertEquals(dh.int_.clazz(), cols[6].getDataType())


    def testAvro(self):
        """
        Check an Avro Kafka subscription creates the right table.
        """

        avro_as_json = \
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

        sys_str = "curl -X POST -H 'Content-type: application/json; artifactType=AVRO' " + \
          "-H 'X-Registry-ArtifactId: share_price_record' -H 'X-Registry-Version: 1' " + \
          "--data-binary '%s' http://registry:8080/api/artifacts" % (avro_as_json)

        r = os.system(sys_str)
        self.assertEquals(0, r)
        
        t = kt.consumeToTable(
            {'bootstrap.servers' : 'redpanda:29092',
             'schema.registry.url' : 'http://registry:8080/api/ccompat'},
            'share_price',
            key = kt.IGNORE,
            value = kt.avro('share_price_record', schema_version='1'),
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
        
