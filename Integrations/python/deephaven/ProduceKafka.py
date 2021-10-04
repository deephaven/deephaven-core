# -*-Python-*-
#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

import collections
import sys
import jpy
import wrapt

import deephaven.Types as dh

from ..conversion_utils import _isJavaType, _isStr, \
    _typeFromName, _dictToProperties, _dictToMap, IDENTITY

# None until the first _defineSymbols() call
_java_type_ = None
_avro_schema_jtype_ = None
_produce_jtype_ = None
IGNORE = None

def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _java_type_,  _avro_schema_jtype_, _produce_jtype_, \
        IGNORE,
    if _java_type_ is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_ = jpy.get_type("io.deephaven.kafka.KafkaTools")
        _stream_table_tools_ = jpy.get_type("io.deephaven.db.v2.StreamTableTools")
        _avro_schema_jtype_ = jpy.get_type("org.apache.avro.Schema")
        _produce_jtype_= jpy.get_type("io.deephaven.kafka.KafkaTools.Produce")
        IGNORE = getattr(_consume_jtype_, 'IGNORE')

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


# Define all of our functionality, if currently possible
try:
    _defineSymbols()
except Exception as e:
    pass



@_passThrough
def produceFromTable(
        t,
        kafka_config:dict,
        topic:str,
        key = None,
        value = None,
):
    """
    Consume from Kafka to a Deephaven table.

    :param t
    :param kafka_config: Dictionary with properties to configure the associated kafka producer.
    :param topic: The topic name
    :param key: A specification for how to map the Key field in Kafka messages.  This should be
       the result of calling one of the methods simple, avro or json in this module,
       or None to obtain a single column specified in the kafka_config param via the 
       keys 'deephaven.key.column.name' for column name and 'deephaven.key.column.type' for
       the column type; both should have string values associated to them.
    :param value: A specification for how to map the Value field in Kafka messages.  This should be
       the result of calling one of the methods simple, avro or json in this module,
       or None to obtain a single column specified in the kafka_config param via the 
       keys 'deephaven.value.column.name' for column name and 'deephaven.value.column.type' for
       the column type; both should have string values associated to them.
    :param table_type: A string specifying the resulting table type: one of 'stream' (default), 'append',
       'stream_map' or 'append_map'.
    :return: A Deephaven live table that will update based on Kafma messages consumed for the given topic.
    :raises: ValueError or TypeError if arguments provided can't be processed.
    """
    return None
