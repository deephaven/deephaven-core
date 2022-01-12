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
        server_name:str,
        db_name:str,
        table_name:str,
        partitions = None,
        ignore_key = False,
        append_only_source = False,
):
    """
    Consume from a Change Data Capture (CDC) Kafka stream (as, eg, produced by Debezium) to a Deephaven table.
    The topic name is calculated concatenatic server_name, db_name and table_name, separate by the '.' character.
    The names of key and or value schemas in schema server are formed by concatenating either "-key"
    or "-value", respectively, to the topic name.

    :param kafka_config: Dictionary with properties to configure the associated kafka consumer and
        also the resulting table.  Once the table-specific properties are stripped, the result is
        passed to the org.apache.kafka.clients.consumer.KafkaConsumer constructor; pass any
        KafkaConsumer specific desired configuration here.  Note this should include the relevant property
        for a schema server URL where the key and/or value Avro necessary schemas are stored.
    :param server_name:  The CDC server name.  This is used to compute the Kafka topic name (first component),
        and also the Avro schema names expected in schema server for either key or value.
    :param db_name:  The CDC database name.  This is used to compute the Kafka topic name (second component),
        and also the Avro schema names expected in schema server for either key or value.
    :param table_name:  The CDC table name.  This is used to compute the Kafka topic name (third component),
        and also the Avro schema names expected in schema server for either key or value.
    :param partitions: Either a sequence of integer partition numbers or the predefined constant
        ALL_PARTITIONS for all partitions.  Defaults to ALL_PARTITIONS if unespecified.
    :param ignore_key: Whether to ignore the key related columns for the CDC stream.
        If true, and the source is not append only, the source table will be treated as if
        the primary key contained all columns.  Defaults to FALSE.
    :param append_only_source: Whether the DHC engine can assume there will be no deletions or updates to
        the source table, only additions.  It is possible to do more efficient handling in this case.
        Defaults to FALSE.
    :return: A Deephaven live table that will update based on the CDC messages consumed for the given topic.
    :raises: ValueError or TypeError if arguments provided can't be processed.
    """

    if partitions is None:
        partitions = ALL_PARTITIONS
    elif isinstance(partitions, collections.Sequence):
        try:
            jarr = jpy.array('int', partitions)
        except Exception as e:
            raise ValueError(
                "when not one of the predefined constants, keyword argument 'partitions' has to " +
                "represent a sequence of integer partition with values >= 0, instead got " +
                str(partitions) + " of type " + type(partitions).__name__
            ) from e
        partitions = _java_type_.partitionFilterFromArray(jarr)
    elif not isinstance(partitions, jpy.JType):
        raise TypeError(
            "argument 'partitions' has to be of str or sequence type, " +
            "or a predefined compatible constant, instead got partitions " +
            str(partitions) + " of type " + type(partitions).__name__)

    kafka_config = _dictToProperties(kafka_config)
    return _java_type_.consumeToTable(
        kafka_config, server_name, db_name, table_name, partitions, ignore_key, append_only_source)
