# -*-Python-*-
#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

import jpy
import wrapt

import deephaven.Types as dh

from deephaven.conversion_utils import _isJavaType, _isStr, \
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

    global _java_type_,  _avro_schema_jtype_, _produce_jtype_, IGNORE
    if _java_type_ is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_ = jpy.get_type("io.deephaven.kafka.KafkaTools")
        _stream_table_tools_ = jpy.get_type("io.deephaven.db.v2.StreamTableTools")
        _avro_schema_jtype_ = jpy.get_type("org.apache.avro.Schema")
        _produce_jtype_= jpy.get_type("io.deephaven.kafka.KafkaTools$Produce")
        IGNORE = getattr(_produce_jtype_, 'IGNORE')

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
        table,
        kafka_config:dict,
        topic:str,
        key,
        value,
        last_by_key_columns:bool = False
):
    """
    Produce to Kafka from a Deephaven table.

    :param table: a Deephaven table used as a source of rows to publish to Kafka.
    :param kafka_config: Dictionary with properties to configure the associated kafka producer.
    :param topic: The topic name
    :param key: A specification for how to map table column(s) to the Key field in produced
           Kafka messages.  This should be the result of calling one of the methods
           simple, avro or json in this module, or the constant IGNORE.
    :param value: A specification for how to map table column(s) to the Value field in produced
           Kafka messages.  This should be the result of calling one of the methods
           simple, avro or json in this module, or the constant IGNORE.
    :param last_by_key_columns:  Whether to publish only the last record for each unique key.
           Ignored if key is IGNORE.  If key is not IGNORE and last_by_key_columns is false,
           it is expected that table updates will not produce any row shifts; that is, the publisher
           expects keyed tables to be streams, add-only, or aggregated.
    :return: A callback object that, when invoked, stops publishing.
             Users should hold to this object to ensure liveleness for publishing
             for as long as this publishing is desired, and once not desired anymore they should
             invoke the callback.
    :raises: ValueError or TypeError if arguments provided can't be processed.
    """

    if not _isStr(topic):
        raise ValueError("argument 'topic' has to be of str type, instead got " + topic)

    if key is None:
        raise ValueError("argument 'key' is None")
    if value is None:
        raise ValueError("argument 'value' is None")
    if key is IGNORE and value is IGNORE:
        raise ValueError(
            "at least one argument for 'key' or 'value' must be different from IGNORE")

    kafka_config = _dictToProperties(kafka_config)
    return _java_type_.produceFromTable(table, kafka_config, topic, key, value, last_by_key_columns)

@_passThrough
def avro(schema, schema_version:str = None, column_names = None):
    """
    Specify an Avro schema to use when producing a Kafka stream from a Deephaven table.

    :param schema:  Either an Avro schema object or a string specifying a schema name for a schema
       registered in a Confluent compatible Schema Server.  When the latter is provided, the
       associated kafka_config dict in the call to consumeToTable should include the key
       'schema.registry.url' with the associated value of the Schema Server URL for fetching the schema
       definition.
    :param schema_version:  If a string schema name is provided, the version to fetch from schema
       service; if not specified, a default of 'latest' is assumed.
    :param column_names: A list of strings each corresponding to an schema field, in order,
       indicating which Deephaven column nanmes to use for the corresponding Avro field,
       or None if the Deephaven columns names are expected to match field names.
    :return:  A Kafka Key or Value spec object to use in a call to produceFromTable.
    :raises:  ValueError, TypeError or Exception if arguments provided can't be processed.
    """
    if _isStr(schema):
        have_actual_schema = False
        if schema_version is None:
            schema_version = "latest"
        elif not _isStr(schema_version):
            raise TypeError(
                "argument 'schema_version' should be of str type, instead got " +
                str(schema_version) + " of type " + type(schema_version).__name__)
    elif isinstance(schema, _avro_schema_jtype_):
        have_actual_schema = True
        if schema_version is not None:
            raise Exception(
                "argument 'schema_version' is only expected if schema is of str type")
    else:
        raise TypeError(
            "'schema' argument expected to be of either " +
            "str type or avro schema type, instead got " + str(schema))

    if have_actual_schema:
        return _produce_jtype_.avroSpec(schema, column_names)
    else:
        return _produce_jtype_.avroSpec(schema, schema_version, column_names)

@_passThrough
def json(column_names = None, field_names = None):
    """
    Specify how to produce JSON data when producing a Kafka stream from a Deephaven table.

    :param column_names: A sequence of Deephaven column names to include in the json output
              as json fields, or None to indicate all columns.
    :param field_names: A squence of field names to use for the corresponding column name in the
              column_names, or None to indicate that all JSON field names will match column names.
    :return:  A Kafka Key or Value spec object to use in a call to produceFromTable.
    :raises:  ValueError, TypeError or Exception if arguments provided can't be processed.
    """
    return _produce_jtype_.jsonSpec(column_names, field_names)


@_passThrough
def simple(column_name:str):
    """
    Specify a single column when producing to a Kafka Key or Value field.

    :param column_name:  A string specifying the Deephaven column name to use.
    :return:  A Kafka Key or Value spec object to use in a call to produceFromTable.
    :raises:  TypeError if arguments provided can't be processed.
    """
    if not _isStr(column_name):
        raise TypeError(
            "'column_name' argument needs to be of str type, instead got " + str(column_name))
    return _produce_jtype_.simpleSpec(column_name)
