#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import os
import unittest
from datetime import datetime

from deephaven import kafka_producer as pk, new_table, time_table
from deephaven.column import string_col, int_col, double_col, datetime_col
from deephaven.stream import kafka
from deephaven.stream.kafka.producer import KeyValueSpec
from tests.testbase import BaseTestCase


def table_helper():
    columns = [
        string_col('Symbol', ['MSFT', 'GOOG', 'AAPL', 'AAPL']),
        string_col('Side', ['B', 'B', 'S', 'B']),
        int_col('Qty', [200, 100, 300, 50]),
        double_col('Price', [210.0, 310.5, 411.0, 411.5])
    ]
    t = new_table(cols=columns)
    return t


class KafkaProducerTestCase(BaseTestCase):
    """
    Test cases for the deephaven.kafka_producer module (performed locally) -
    """

    def test_basic_constants(self):
        """
        Check that the basic constants are imported and visible.
        """
        self.assertIsNotNone(KeyValueSpec.IGNORE)

    def test_simple_spec(self):
        """
        Check a simple Kafka producer works without errors
        """
        t = new_table(cols=[double_col('Price', [10.0, 10.5, 11.0, 11.5])])
        cleanup = pk.produce(
            t,
            {'bootstrap.servers': 'redpanda:29092'},
            'orders',
            key_spec=KeyValueSpec.IGNORE,
            value_spec=pk.simple_spec('Price')
        )

        self.assertIsNotNone(cleanup)
        cleanup()

    def test_simple_spec_topic_col_no_default_topic(self):
        """
        Check a simple Kafka producer works with a topic column but no default topic
        """
        t = new_table(cols=[
            string_col('Topic', ['orders_a', 'orders_b', 'orders_a', 'orders_b']),
            double_col('Price', [10.0, 10.5, 11.0, 11.5])
        ])
        cleanup = pk.produce(
            t,
            {'bootstrap.servers': 'redpanda:29092'},
            None,
            key_spec=KeyValueSpec.IGNORE,
            value_spec=pk.simple_spec('Price'),
            topic_col='Topic'
        )

        self.assertIsNotNone(cleanup)
        cleanup()

    def test_simple_spec_topic_col_default_topic(self):
        """
        Check a simple Kafka producer works with a topic column and a default topic
        """
        t = new_table(cols=[
            string_col('Topic', ['orders_a', None, 'orders_a', 'orders_b']),
            double_col('Price', [10.0, 10.5, 11.0, 11.5])
        ])
        cleanup = pk.produce(
            t,
            {'bootstrap.servers': 'redpanda:29092'},
            'orders',
            key_spec=KeyValueSpec.IGNORE,
            value_spec=pk.simple_spec('Price'),
            topic_col='Topic'
        )

        self.assertIsNotNone(cleanup)
        cleanup()

    def test_simple_spec_default_partition(self):
        """
        Check a simple Kafka producer works with a default partition
        """
        t = new_table(cols=[
            double_col('Price', [10.0, 10.5, 11.0, 11.5])]
        )
        cleanup = pk.produce(
            t,
            {'bootstrap.servers': 'redpanda:29092'},
            "orders",
            key_spec=KeyValueSpec.IGNORE,
            value_spec=pk.simple_spec('Price'),
            partition=0
        )

        self.assertIsNotNone(cleanup)
        cleanup()

    def test_simple_spec_partition_col_no_default_partition(self):
        """
        Check a simple Kafka producer works with a partition column
        """
        t = new_table(cols=[
            int_col('Partition', [0, 0, 0, 0]),
            double_col('Price', [10.0, 10.5, 11.0, 11.5])
        ])
        cleanup = pk.produce(
            t,
            {'bootstrap.servers': 'redpanda:29092'},
            "orders",
            key_spec=KeyValueSpec.IGNORE,
            value_spec=pk.simple_spec('Price'),
            partition_col='Partition'
        )

        self.assertIsNotNone(cleanup)
        cleanup()

    def test_simple_spec_partition_col_default_partition(self):
        """
        Check a simple Kafka producer works with a partition column and default partition
        """
        t = new_table(cols=[
            int_col('Partition', [0, 0, None, 0]),
            double_col('Price', [10.0, 10.5, 11.0, 11.5])
        ])
        cleanup = pk.produce(
            t,
            {'bootstrap.servers': 'redpanda:29092'},
            "orders",
            key_spec=KeyValueSpec.IGNORE,
            value_spec=pk.simple_spec('Price'),
            partition=0,
            partition_col='Partition'
        )

        self.assertIsNotNone(cleanup)
        cleanup()

    def test_simple_spec_timestamp_col(self):
        """
        Check a simple Kafka producer works with a timestamp column
        """
        t = new_table(cols=[
            datetime_col('Timestamp', [datetime.now(), datetime.now(), None, datetime.now()]),
            double_col('Price', [10.0, 10.5, 11.0, 11.5])
        ])
        cleanup = pk.produce(
            t,
            {'bootstrap.servers': 'redpanda:29092'},
            "orders",
            key_spec=KeyValueSpec.IGNORE,
            value_spec=pk.simple_spec('Price'),
            timestamp_col='Timestamp'
        )

        self.assertIsNotNone(cleanup)
        cleanup()

    def test_json_spec_only_columns(self):
        t = table_helper()
        cleanup = pk.produce(
            t,
            {'bootstrap.servers': 'redpanda:29092'},
            'orders',
            key_spec=KeyValueSpec.IGNORE,
            value_spec=pk.json_spec(['Symbol', 'Price']),
            last_by_key_columns=False
        )

        self.assertIsNotNone(cleanup)
        cleanup()

    def test_json_spec_all_arguments(self):
        t = table_helper()
        cleanup = pk.produce(
            t,
            {'bootstrap.servers': 'redpanda:29092'},
            'orders',
            key_spec=KeyValueSpec.IGNORE,
            value_spec=pk.json_spec(
                ['Symbol', 'Price'],
                mapping={'Symbol': 'jSymbol', 'Price': 'jPrice'},
                timestamp_field='jTs'
            ),
            last_by_key_columns=False
        )

        self.assertIsNotNone(cleanup)
        cleanup()

    def test_avro_spec(self):
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
        self.assertEqual(0, r)

        kafka_config = {
            'bootstrap.servers': 'redpanda:29092',
            'schema.registry.url': 'http://redpanda:8081'
        }
        topic = 'share_price_timestamped'
        t = table_helper()
        cleanup = pk.produce(
            t,
            kafka_config,
            topic,
            key_spec=KeyValueSpec.IGNORE,
            value_spec=pk.avro_spec(
                'share_price_timestamped_record',
                timestamp_field='Timestamp'
            ),
            last_by_key_columns=False
        )

        topics = kafka.topics(kafka_config)
        self.assertTrue(len(topics) > 0)
        self.assertIn(topic, topics)

        self.assertIsNotNone(cleanup)
        cleanup()

    def test_not_publish_initial(self):
        """
        Check a simple Kafka producer with publish_initial=False works without errors
        """
        # Note: using long column since there is no simple / native kafka Instant serializer
        t = time_table("PT1s").view(["TimestampNanos=epochNanos(Timestamp)"])
        cleanup = pk.produce(
            t,
            {'bootstrap.servers': 'redpanda:29092'},
            'my_timestamps',
            key_spec=KeyValueSpec.IGNORE,
            value_spec=pk.simple_spec('TimestampNanos'),
            publish_initial=False,
        )

        self.assertIsNotNone(cleanup)
        cleanup()


if __name__ == '__main__':
    unittest.main()
