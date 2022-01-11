#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import os
import unittest

from deephaven2 import kafka_consumer as ck
from tests.testbase import BaseTestCase
from deephaven2 import dtypes


class KafkaConsumerTestCase(BaseTestCase):

    def _assert_common_cols(self, cols):
        self.assertEquals("KafkaPartition", cols[0].name)
        self.assertEquals(dtypes.int32, cols[0].data_type)
        self.assertEquals("KafkaOffset", cols[1].name)
        self.assertEquals(dtypes.long, cols[1].data_type)
        self.assertEquals("KafkaTimestamp", cols[2].name)
        self.assertEquals(dtypes.DateTime, cols[2].data_type)

    def test_basic_constants(self):
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

    def test_simple(self):
        """
        Check a simple Kafka subscription creates the right table.
        """
        t = ck.consume(
            {'bootstrap.servers': 'redpanda:29092'},
            'orders',
            key=ck.IGNORE,
            value=ck.simple('Price', dtypes.double))

        cols = t.columns
        self.assertEquals(4, len(cols))
        self._assert_common_cols(cols)
        self.assertEquals("Price", cols[3].name)
        self.assertEquals(dtypes.double, cols[3].data_type)

    def test_json(self):
        """
        Check a JSON Kafka subscription creates the right table.
        """

        t = ck.consume(
            {'bootstrap.servers': 'redpanda:29092'},
            'orders',
            key=ck.IGNORE,
            value=ck.json(
                [('Symbol', dtypes.string),
                 ('Side', dtypes.string),
                 ('Price', dtypes.double),
                 ('Qty', dtypes.int_),
                 ('Tstamp', dtypes.DateTime)],
                mapping={
                    'jsymbol': 'Symbol',
                    'jside': 'Side',
                    'jprice': 'Price',
                    'jqty': 'Qty',
                    'jts': 'Tstamp'
                }
            ),
            table_type='append'
        )

        cols = t.columns
        self.assertEquals(8, len(cols))
        self._assert_common_cols(cols)

        self.assertEquals("Symbol", cols[3].name)
        self.assertEquals(dtypes.string, cols[3].data_type)
        self.assertEquals("Side", cols[4].name)
        self.assertEquals(dtypes.string, cols[4].data_type)
        self.assertEquals("Price", cols[5].name)
        self.assertEquals(dtypes.double, cols[5].data_type)
        self.assertEquals("Qty", cols[6].name)
        self.assertEquals(dtypes.int_, cols[6].data_type)
        self.assertEquals("Tstamp", cols[7].name)
        self.assertEquals(dtypes.DateTime, cols[7].data_type)

    def test_avro(self):
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
        self.assertEqual(0, r)

        with self.subTest(msg='straight schema, no mapping'):
            t = ck.consume(
                {
                    'bootstrap.servers': 'redpanda:29092',
                    'schema.registry.url': 'http://redpanda:8081'
                },
                'share_price',
                key=ck.IGNORE,
                value=ck.avro('share_price_record', schema_version='1'),
                table_type='append'
            )

            cols = t.columns
            self.assertEquals(7, len(cols))
            self._assert_common_cols(cols)

            self.assertEquals("Symbol", cols[3].name)
            self.assertEquals(dtypes.string, cols[3].data_type)
            self.assertEquals("Side", cols[4].name)
            self.assertEquals(dtypes.string, cols[4].data_type)
            self.assertEquals("Qty", cols[5].name)
            self.assertEquals(dtypes.int_, cols[5].data_type)
            self.assertEquals("Price", cols[6].name)
            self.assertEquals(dtypes.double, cols[6].data_type)

        with self.subTest(msg='mapping_only (filter out some schema fields)'):
            m = {'Symbol': 'Ticker', 'Price': 'Dollars'}
            t = ck.consume(
                {
                    'bootstrap.servers': 'redpanda:29092',
                    'schema.registry.url': 'http://redpanda:8081'
                },
                'share_price',
                key=ck.IGNORE,
                value=ck.avro('share_price_record', mapping_only=m),
                table_type='append'
            )

            cols = t.columns
            self.assertEquals(5, len(cols))
            self._assert_common_cols(cols)

            self.assertEquals("Ticker", cols[3].name)
            self.assertEquals(dtypes.string, cols[3].data_type)
            self.assertEquals("Dollars", cols[4].name)
            self.assertEquals(dtypes.double, cols[4].data_type)

        with self.subTest(msg='mapping (rename some fields)'):
            m = {'Symbol': 'Ticker', 'Qty': 'Quantity'}
            t = ck.consume(
                {
                    'bootstrap.servers': 'redpanda:29092',
                    'schema.registry.url': 'http://redpanda:8081'
                },
                'share_price',
                key=ck.IGNORE,
                value=ck.avro('share_price_record', mapping=m),
                table_type='append'
            )

            cols = t.columns
            self.assertEquals(7, len(cols))
            self._assert_common_cols(cols)

            self.assertEquals("Ticker", cols[3].name)
            self.assertEquals(dtypes.string, cols[3].data_type)
            self.assertEquals("Side", cols[4].name)
            self.assertEquals(dtypes.string, cols[4].data_type)
            self.assertEquals("Quantity", cols[5].name)
            self.assertEquals(dtypes.int_, cols[5].data_type)
            self.assertEquals("Price", cols[6].name)
            self.assertEquals(dtypes.double, cols[6].data_type)


if __name__ == "__main__":
    unittest.main()
