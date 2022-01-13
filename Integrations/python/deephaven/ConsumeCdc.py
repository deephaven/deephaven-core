# -*-Python-*-
#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

import collections
import jpy
import wrapt
import deephaven.ConsumeKafka as ck

from deephaven.conversion_utils import _dictToProperties, _isStr

# None until the first _defineSymbols() call
_java_type_ = None
ALL_PARTITIONS = None

def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _java_type_, ALL_PARTITIONS
    if _java_type_ is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_ = jpy.get_type("io.deephaven.kafka.CdcTools")
        ck._defineSymbols()
        ALL_PARTITIONS = ck.ALL_PARTITIONS


        # every module method should be decorated with @_passThrough
@wrapt.decorator
def _passThrough(wrapped, instance, args, kwargs):
    """
    For decoration of module methods, to define necessary symbols at runtime

    :param wrapped: the method to be decorated
    :param instance: the object to which the wrapped function was bound when it was called
    :param args: the argument list for `wrapped`
    :param kwargs: the keyword argument dictionary for `wrapped`
    :return: the decorated version of the method
    """

    _defineSymbols()
    return wrapped(*args, **kwargs)

@_passThrough
def consumeToTable(
        kafka_config:dict,
        cdc_spec,
        partitions = None,
        as_stream_table = False,
        drop_columns = None,
):
    """
    Consume from a Change Data Capture (CDC) Kafka stream (as, eg, produced by Debezium)
    tracking the underlying database table to a Deephaven table.

    :param kafka_config: Dictionary with properties to configure the associated kafka consumer and
         also the resulting table.  Passed to the org.apache.kafka.clients.consumer.KafkaConsumer constructor;
         pass any KafkaConsumer specific desired configuration here.
         Note this should include the relevant property for a schema server URL where the
         key and/or value Avro necessary schemas are stored.
    :param cdc_spec:  A CDC Spec opaque object obtained from calling either the cdc_explict_spec method
                      or the cdc_short_spec method
    :param partitions: Either a sequence of integer partition numbers or the predefined constant
         ALL_PARTITIONS for all partitions.  Defaults to ALL_PARTITIONS if unspecified.
    :param as_stream_table:  If true, produce a streaming table of changed rows keeping
         the CDC 'op' column indicating the type of column change; if false, return
         a DHC ticking table that tracks the underlying database table through the CDC Stream.
    :param drop_columns: A sequence of column names to omit from the resulting DHC table.
         Note that only columns not included in the primary key for the table can be dropped at this stage;
         you can chain a drop column operation after this call if you need to do this.
    :return: A Deephaven live table that will update based on the CDC messages consumed for the given topic.
    :raises: ValueError or TypeError if arguments provided can't be processed.
    """

    partitions = ck._jpy_partitions(partitions)
    kafka_config = _dictToProperties(kafka_config)
    return _java_type_.consumeToTable(
        kafka_config,
        cdc_spec,
        partitions,
        as_stream_table,
        drop_columns)

@_passThrough
def consumeRawToTable(
        kafka_config:dict,
        cdc_spec,
        partitions = None,
        table_type:str = 'stream'
):
    """
    Consume the raw events from a Change Data Capture (CDC) Kafka stream to a Deephaven table.

    :param kafka_config: Dictionary with properties to configure the associated kafka consumer and
        also the resulting table.  Passed to the org.apache.kafka.clients.consumer.KafkaConsumer constructor;
        pass any KafkaConsumer specific desired configuration here.
        Note this should include the relevant property for a schema server URL where the
        key and/or value Avro necessary schemas are stored.
    :param cdc_spec:     A CDC Spec opaque object obtained from calling either the cdc_explict_spec method
                         or the cdc_short_spec method
    :param partitions:   Either a sequence of integer partition numbers or the predefined constant
        ALL_PARTITIONS for all partitions.  Defaults to ALL_PARTITIONS if unspecified.
    :param table_type:   A string specifying the resulting table type: one of 'stream' (default), 'append',
       'stream_map' or 'append_map'.
    :return: A Deephaven live table for the raw CDC events.
    """
    partitions = ck._jpy_partitions(partitions)
    kafka_config = _dictToProperties(kafka_config)
    table_type_enum = ck._jpy_table_type(table_type)
    return _java_type_.consumeRawToTable(kafka_config, cdc_spec, partitions, table_type_enum)

@_passThrough
def cdc_long_spec(
        topic:str,
        key_schema_name:str,
        key_schema_version:str,
        value_schema_name:str,
        value_schema_version:str,
):
    """
    Create a CdcSpec opaque object (necessary for one argument in a call to consume*ToTable)
    via explicitly specifying all configuration options.

    :param topic:  The Kafka topic for the CDC events associated to the desired table data.
    :param key_schema_name:  The schema name for the Key Kafka field in the CDC events for the topic.
        This schema should include definitions for the columns forming the PRIMARY KEY of the underlying table.
        This schema name will be looked up in a schema server.
    :param key_schema_version:  The version for the Key schema to look up in schema server.
        None or "latest" implies using the latest version when Key is not ignored.
    :param value_schema_name:  The schema name for the Value Kafka field in the CDC events for the topic.
        This schema should include definitions for all the columns of the underlying table.
        This schema name will be looked up in a schema server.
    :param value_schema_version:  The version for the Value schema to look up in schema server.
        None or "latest" implies using the latest version.
    :return: A CDCSpec object representing the inputs.
    """
    return _java_type_.cdcLongSpec(topic, key_schema_name, key_schema_version, value_schema_name, value_schema_version)

@_passThrough
def cdc_short_spec(
        server_name:str,
        db_name:str,
        table_name:str,
):
    """
    Create a CdcSpec opaque object (necessary for one argument in a call to consume*ToTable)
    in the debezium style, specifying server name, database name and table name.
    The topic name, and key and value schema names are implied by convention:
      - Topic is the concatenation of the arguments using "." as separator.
      - Key schema name is topic with a "-key" suffix added.
      - Value schema name is topic with a "-value" suffix added.

    :param server_name:  The server_name configuration value used when the CDC Stream was created.
    :param db_name:      The database name configuration value used when the CDC Stream was created.
    :param table_name:   The table name configuration value used when the CDC Stream was created.
    :return: A CDCSpec object representing the inputs.
    """
    return _java_type_.cdcShortSpec(server_name, db_name, table_name)
