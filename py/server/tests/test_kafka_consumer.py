#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import os
import unittest
from datetime import datetime

from deephaven import kafka_consumer as ck
from deephaven.stream.kafka.consumer import TableType, KeyValueSpec
from tests.testbase import BaseTestCase
from deephaven import dtypes
from deephaven.json.jackson import provider as jackson_provider

class KafkaConsumerTestCase(BaseTestCase):

    def _assert_common_cols(self, cols):
        self.assertEqual("KafkaPartition", cols[0].name)
        self.assertEqual(dtypes.int32, cols[0].data_type)
        self.assertEqual("KafkaOffset", cols[1].name)
        self.assertEqual(dtypes.long, cols[1].data_type)
        self.assertEqual("KafkaTimestamp", cols[2].name)
        self.assertEqual(dtypes.Instant, cols[2].data_type)

    def test_basic_constants(self):
        """
        Check that the basic constants are imported and visible.
        """
        self.assertIsNotNone(ck.SEEK_TO_BEGINNING)
        self.assertIsNotNone(ck.DONT_SEEK)
        self.assertIsNotNone(ck.SEEK_TO_END)
        self.assertIsNotNone(ck.ALL_PARTITIONS_SEEK_TO_BEGINNING)
        self.assertIsNotNone(ck.ALL_PARTITIONS_SEEK_TO_END)
        self.assertIsNotNone(ck.ALL_PARTITIONS_DONT_SEEK)

    def test_simple_spec(self):
        """
        Check a simple Kafka subscription creates the right table.
        """
        t = ck.consume(
            {'bootstrap.servers': 'redpanda:29092'},
            'orders',
            key_spec=KeyValueSpec.IGNORE,
            value_spec=ck.simple_spec('Price', dtypes.double))

        cols = t.columns
        self.assertEqual(4, len(cols))
        self._assert_common_cols(cols)
        self.assertEqual("Price", cols[3].name)
        self.assertEqual(dtypes.double, cols[3].data_type)

    def test_json_spec(self):
        """
        Check a JSON Kafka subscription creates the right table.
        """
        value_specs = [ck.json_spec(
            col_defs=[('Symbol', dtypes.string),
                      ('Side', dtypes.string),
                      ('Price', dtypes.double),
                      ('Qty', dtypes.int64),
                      ('Tstamp', dtypes.Instant)],
            mapping={
                'jsymbol': 'Symbol',
                'jside': 'Side',
                'jprice': 'Price',
                'jqty': 'Qty',
                'jts': 'Tstamp'
            }
        ), ck.json_spec(
            col_defs={
                'Symbol': dtypes.string,
                'Side': dtypes.string,
                'Price': dtypes.double,
                'Qty': dtypes.int64,
                'Tstamp': dtypes.Instant
            },
            mapping={
                'jsymbol': 'Symbol',
                'jside': 'Side',
                'jprice': 'Price',
                'jqty': 'Qty',
                'jts': 'Tstamp'
            }
        ), ck.object_processor_spec(jackson_provider({
            'Symbol': str,
            'Side': str,
            'Price': float,
            'Qty': int,
            'Tstamp': datetime
        }))]

        for value_spec in value_specs:
            t = ck.consume(
                {'bootstrap.servers': 'redpanda:29092'},
                'orders',
                key_spec=KeyValueSpec.IGNORE,
                value_spec=value_spec,
                table_type=TableType.append()
            )

            cols = t.columns
            self.assertEqual(8, len(cols))
            self._assert_common_cols(cols)

            self.assertEqual("Symbol", cols[3].name)
            self.assertEqual(dtypes.string, cols[3].data_type)
            self.assertEqual("Side", cols[4].name)
            self.assertEqual(dtypes.string, cols[4].data_type)
            self.assertEqual("Price", cols[5].name)
            self.assertEqual(dtypes.double, cols[5].data_type)
            self.assertEqual("Qty", cols[6].name)
            self.assertEqual(dtypes.int64, cols[6].data_type)
            self.assertEqual("Tstamp", cols[7].name)
            self.assertEqual(dtypes.Instant, cols[7].data_type)

    def test_protobuf_spec(self):
        """
        Check an Protobuf Kafka subscription creates the right table.
        """
        schema = """syntax = "proto3";
import "google/protobuf/timestamp.proto";

package io.deephaven.example;

message Sub {
    string first = 1;
    string last = 2;
}

message SomeMessage {
  google.protobuf.Timestamp ts = 1;
  string name = 2;
  int32 foo = 3;
  double bar = 4;
  Sub sub = 5;
}
"""
        schema_normalized = schema.replace("\n", " ").replace('"', '\\"')
        schema_str = '{ "schemaType": "PROTOBUF", "schema" : "%s" }' % schema_normalized
        sys_str = f"""
curl -X POST \
    -H 'Content-type: application/vnd.schemaregistry.v1+json' \
    --data-binary '{schema_str}' \
    http://redpanda:8081/subjects/io%2Fdeephaven%2Fexample%2FMySchema.proto/versions
"""

        r = os.system(sys_str)
        self.assertEqual(0, r)

        def consume(value_spec):
            return ck.consume(
                {
                    "bootstrap.servers": "redpanda:29092",
                    "schema.registry.url": "http://redpanda:8081",
                },
                "my_pb_topic",
                key_spec=KeyValueSpec.IGNORE,
                value_spec=value_spec,
                table_type=TableType.append(),
            )

        with self.subTest(msg="regular"):
            t = consume(
                ck.protobuf_spec(
                    schema="io/deephaven/example/MySchema.proto",
                    schema_version=1,
                    schema_message_name="io.deephaven.example.SomeMessage",
                )
            )
            cols = t.columns
            self.assertEqual(9, len(cols))
            self._assert_common_cols(cols)
            self.assertEqual("ts", cols[3].name)
            self.assertEqual(dtypes.Instant, cols[3].data_type)
            self.assertEqual("name", cols[4].name)
            self.assertEqual(dtypes.string, cols[4].data_type)
            self.assertEqual("foo", cols[5].name)
            self.assertEqual(dtypes.int32, cols[5].data_type)
            self.assertEqual("bar", cols[6].name)
            self.assertEqual(dtypes.double, cols[6].data_type)
            self.assertEqual("sub_first", cols[7].name)
            self.assertEqual(dtypes.string, cols[7].data_type)
            self.assertEqual("sub_last", cols[8].name)
            self.assertEqual(dtypes.string, cols[8].data_type)

        with self.subTest(msg="include /foo /bar"):
            t = consume(
                ck.protobuf_spec(
                    schema="io/deephaven/example/MySchema.proto",
                    schema_version=1,
                    schema_message_name="io.deephaven.example.SomeMessage",
                    include=["/foo", "/bar"],
                )
            )
            cols = t.columns
            self.assertEqual(5, len(cols))
            self._assert_common_cols(cols)
            self.assertEqual("foo", cols[3].name)
            self.assertEqual(dtypes.int32, cols[3].data_type)
            self.assertEqual("bar", cols[4].name)
            self.assertEqual(dtypes.double, cols[4].data_type)


        with self.subTest(msg="include /ts /sub/*"):
            t = consume(
                ck.protobuf_spec(
                    schema="io/deephaven/example/MySchema.proto",
                    schema_version=1,
                    schema_message_name="io.deephaven.example.SomeMessage",
                    include=["/ts", "/sub/*"],
                )
            )
            cols = t.columns
            self.assertEqual(6, len(cols))
            self._assert_common_cols(cols)
            self.assertEqual("ts", cols[3].name)
            self.assertEqual(dtypes.Instant, cols[3].data_type)
            self.assertEqual("sub_first", cols[4].name)
            self.assertEqual(dtypes.string, cols[4].data_type)
            self.assertEqual("sub_last", cols[5].name)
            self.assertEqual(dtypes.string, cols[5].data_type)

    def test_avro_spec(self):
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
                key_spec=KeyValueSpec.IGNORE,
                value_spec=ck.avro_spec('share_price_record', schema_version='1'),
                table_type=TableType.append()
            )

            cols = t.columns
            self.assertEqual(7, len(cols))
            self._assert_common_cols(cols)

            self.assertEqual("Symbol", cols[3].name)
            self.assertEqual(dtypes.string, cols[3].data_type)
            self.assertEqual("Side", cols[4].name)
            self.assertEqual(dtypes.string, cols[4].data_type)
            self.assertEqual("Qty", cols[5].name)
            self.assertEqual(dtypes.int32, cols[5].data_type)
            self.assertEqual("Price", cols[6].name)
            self.assertEqual(dtypes.double, cols[6].data_type)

        with self.subTest(msg='mapping_only (filter out some schema fields)'):
            m = {'Symbol': 'Ticker', 'Price': 'Dollars'}
            t = ck.consume(
                {
                    'bootstrap.servers': 'redpanda:29092',
                    'schema.registry.url': 'http://redpanda:8081'
                },
                'share_price',
                key_spec=KeyValueSpec.IGNORE,
                value_spec=ck.avro_spec('share_price_record', mapping=m, mapped_only=True),
                table_type=TableType.append()
            )

            cols = t.columns
            self.assertEqual(5, len(cols))
            self._assert_common_cols(cols)

            self.assertEqual("Ticker", cols[3].name)
            self.assertEqual(dtypes.string, cols[3].data_type)
            self.assertEqual("Dollars", cols[4].name)
            self.assertEqual(dtypes.double, cols[4].data_type)

        with self.subTest(msg='mapping (rename some fields)'):
            m = {'Symbol': 'Ticker', 'Qty': 'Quantity'}
            t = ck.consume(
                {
                    'bootstrap.servers': 'redpanda:29092',
                    'schema.registry.url': 'http://redpanda:8081'
                },
                'share_price',
                key_spec=KeyValueSpec.IGNORE,
                value_spec=ck.avro_spec('share_price_record', mapping=m),
                table_type=TableType.append()
            )

            cols = t.columns
            self.assertEqual(7, len(cols))
            self._assert_common_cols(cols)

            self.assertEqual("Ticker", cols[3].name)
            self.assertEqual(dtypes.string, cols[3].data_type)
            self.assertEqual("Side", cols[4].name)
            self.assertEqual(dtypes.string, cols[4].data_type)
            self.assertEqual("Quantity", cols[5].name)
            self.assertEqual(dtypes.int32, cols[5].data_type)
            self.assertEqual("Price", cols[6].name)
            self.assertEqual(dtypes.double, cols[6].data_type)

    def test_deprecated_table_types(self):
        """
        Tests to make sure deprecated TableTypes are equivalent
        """
        self.assertEqual(TableType.append(), TableType.Append)
        self.assertEqual(TableType.blink(), TableType.Stream)
        self.assertEqual(TableType.blink(), TableType.stream())

    def test_table_types(self):
        """
        Tests TableType construction
        """
        _ = TableType.append()
        _ = TableType.blink()
        _ = TableType.ring(4096)

    def test_json_spec_partitioned_table(self):
        pt = ck.consume_to_partitioned_table(
            {'bootstrap.servers': 'redpanda:29092'},
            'orders',
            key_spec=KeyValueSpec.IGNORE,
            value_spec=ck.json_spec(
                [('Symbol', dtypes.string),
                 ('Side', dtypes.string),
                 ('Price', dtypes.double),
                 ('Qty', dtypes.int64),
                 ('Tstamp', dtypes.Instant)],
                mapping={
                    'jsymbol': 'Symbol',
                    'jside': 'Side',
                    'jprice': 'Price',
                    'jqty': 'Qty',
                    'jts': 'Tstamp'
                }
            ),
            table_type=TableType.append()
        )

        cols = pt.constituent_table_columns
        self.assertEqual(8, len(cols))
        self._assert_common_cols(cols)

        self.assertEqual("Symbol", cols[3].name)
        self.assertEqual(dtypes.string, cols[3].data_type)
        self.assertEqual("Side", cols[4].name)
        self.assertEqual(dtypes.string, cols[4].data_type)
        self.assertEqual("Price", cols[5].name)
        self.assertEqual(dtypes.double, cols[5].data_type)
        self.assertEqual("Qty", cols[6].name)
        self.assertEqual(dtypes.int64, cols[6].data_type)
        self.assertEqual("Tstamp", cols[7].name)
        self.assertEqual(dtypes.Instant, cols[7].data_type)


if __name__ == "__main__":
    unittest.main()
