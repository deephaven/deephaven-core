#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module provides Change Data Capture(CDC) support for streaming relational database changes into Deephaven
tables. """

from typing import Dict, List

import jpy

from deephaven import DHError
from deephaven.jcompat import j_properties
from deephaven._wrapper import JObjectWrapper
from deephaven.stream.kafka.consumer import j_partitions, TableType
from deephaven.table import Table

_JCdcTools = jpy.get_type("io.deephaven.kafka.CdcTools")
_JCDCSpec = jpy.get_type("io.deephaven.kafka.CdcTools$CdcSpec")


class CDCSpec(JObjectWrapper):
    """ A specification for how to consume a CDC Kafka stream. """
    j_object_type = _JCDCSpec

    @property
    def j_object(self) -> jpy.JType:
        return self._j_spec

    def __init__(self, j_spec):
        self._j_spec = j_spec


def consume(
        kafka_config: Dict,
        cdc_spec: CDCSpec,
        partitions: List[int] = None,
        stream_table: bool = False,
        cols_to_drop: List[str] = None,
) -> Table:
    """ Consume from a Change Data Capture (CDC) Kafka stream (as, eg, produced by Debezium), tracking the underlying
    database table to a Deephaven table.

    Args:
        kafka_config (Dict): configuration for the associated kafka consumer and also the resulting table. Passed
            to the org.apache.kafka.clients.consumer.KafkaConsumer constructor; pass any KafkaConsumer specific desired
            configuration here. Note this should include the relevant property for a schema server URL where the key
            and/or value Avro necessary schemas are stored.
        cdc_spec (CDCSpec): a CDCSpec obtained from calling either the cdc_long_spec or the cdc_short_spec function
        partitions (List[int]): a list of integer partition numbers, default is None indicating all partitions
        stream_table (bool):  if true, produce a streaming table of changed rows keeping the CDC 'op' column
            indicating the type of column change; if false, return a Deephaven ticking table that tracks the underlying
            database table through the CDC Stream.
        cols_to_drop (List[str]): a list of column names to omit from the resulting DHC table. Note that only columns
            not included in the primary key for the table can be dropped at this stage; you can chain a drop column
            operation after this call if you need to do this.

    Returns:
        a Deephaven live table that will update based on the CDC messages consumed for the given topic

    Raises:
        DHError
    """
    try:
        partitions = j_partitions(partitions)
        kafka_config = j_properties(kafka_config)
        return Table(
            j_table=_JCdcTools.consumeToTable(kafka_config, cdc_spec.j_object, partitions, stream_table, cols_to_drop))
    except Exception as e:
        raise DHError(e, "failed to consume a CDC stream.") from e


def consume_raw(
        kafka_config: dict,
        cdc_spec: CDCSpec,
        partitions=None,
        table_type: TableType = TableType.stream(),
) -> Table:
    """ Consume the raw events from a Change Data Capture (CDC) Kafka stream to a Deephaven table.

    Args:
        kafka_config (Dict): configuration for the associated kafka consumer and also the resulting table. Passed
            to the org.apache.kafka.clients.consumer.KafkaConsumer constructor; pass any KafkaConsumer specific desired
            configuration here. Note this should include the relevant property for a schema server URL where the key
            and/or value Avro necessary schemas are stored.
        cdc_spec (CDCSpec): a CDCSpec obtained from calling either the cdc_long_spec or the cdc_short_spec function
        partitions (List[int]): a list of integer partition numbers, default is None indicating all partitions
        table_type (TableType): a TableType enum, default is TableType.stream()

    Returns:
        a Deephaven live table for the raw CDC events

    Raises:
        DHError
    """
    try:
        partitions = j_partitions(partitions)
        kafka_config = j_properties(kafka_config)
        table_type_enum = table_type.value
        return Table(j_table=_JCdcTools.consumeRawToTable(kafka_config, cdc_spec.j_object, partitions, table_type_enum))
    except Exception as e:
        raise DHError(e, "failed to consume a raw CDC stream.") from e


def cdc_long_spec(
        topic: str,
        key_schema_name: str,
        key_schema_version: str,
        value_schema_name: str,
        value_schema_version: str,
) -> CDCSpec:
    """ Creates a CDCSpec with all the required configuration options.

    Args:
        topic (str):  the Kafka topic for the CDC events associated to the desired table data.
        key_schema_name (str):  the schema name for the Key Kafka field in the CDC events for the topic. This schema
            should include definitions for the columns forming the PRIMARY KEY of the underlying table. This schema name
            will be looked up in a schema server.
        key_schema_version (str):  the version for the Key schema to look up in schema server. None or "latest"
            implies using the latest version when Key is not ignored.
        value_schema_name (str):  the schema name for the Value Kafka field in the CDC events for the topic. This
            schema should include definitions for all the columns of the underlying table. This schema name will be
            looked up in a schema server.
        value_schema_version (str):  the version for the Value schema to look up in schema server. None or "latest"
            implies using the latest version.

    Returns:
        a CDCSpec

    Raises:
        DHError
    """
    try:
        return CDCSpec(j_spec=_JCdcTools.cdcLongSpec(topic, key_schema_name, key_schema_version, value_schema_name,
                                                     value_schema_version))
    except Exception as e:
        raise DHError(e, "failed to create a CDC spec in cdc_long_spec.") from e


def cdc_short_spec(server_name: str, db_name: str, table_name: str):
    """ Creates a CDCSpec in the debezium style from the provided server name, database name and table name.

    The topic name, and key and value schema names are implied by convention:
      - Topic is the concatenation of the arguments using "." as separator.
      - Key schema name is topic with a "-key" suffix added.
      - Value schema name is topic with a "-value" suffix added.

    Args:
        server_name (str):  the server_name configuration value used when the CDC Stream was created
        db_name (str): the database name configuration value used when the CDC Stream was created
        table_name (str): the table name configuration value used when the CDC Stream was created

    Returns:
        a CDCSpec

    Raises:
        DHError
    """
    try:
        return CDCSpec(j_spec=_JCdcTools.cdcShortSpec(server_name, db_name, table_name))
    except Exception as e:
        raise DHError(e, "failed to create a CDC spec in cdc_short_spec.") from e
