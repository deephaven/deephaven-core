# -*-Python-*-
#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

import jpy
import wrapt

import deephaven.Types as dh

from deephaven.conversion_utils import _isJavaType, _isStr, \
    _typeFromName, _dictToProperties, _dictToMap, _seqToSet

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
        _stream_table_tools_ = jpy.get_type("io.deephaven.engine.table.impl.StreamTableTools")
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
    :return: A callback that, when invoked, stops publishing and cleans up
             subscriptions and resources.
             Users should hold to this callback to ensure liveness for publishing
             for as long as this publishing is desired, and once not desired anymore they should
             invoke it.
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
    runnable = _java_type_.produceFromTable(table, kafka_config, topic, key, value, last_by_key_columns)
    def cleanup():
        runnable.run()
    return cleanup

@_passThrough
def avro(schema, schema_version:str = None, field_to_col_mapping = None, timestamp_field = None):
    """
    Specify an Avro schema to use when producing a Kafka stream from a Deephaven table.

    :param schema:  Either an Avro schema object or a string specifying a schema name for a schema
       registered in a Confluent compatible Schema Server.  When the latter is provided, the
       associated kafka_config dict in the call to consumeToTable should include the key
       'schema.registry.url' with the associated value of the Schema Server URL for fetching the schema
       definition.
    :param schema_version:  If a string schema name is provided, the version to fetch from schema
       service; if not specified, a default of 'latest' is assumed.
    :param field_to_col_mapping: A dict mapping field names in the schema to column names in the Deephaven table.
       Any fields in the schema not present in the dict as keys are mapped to columns of the same name.
       If this argument is None, all schema fields are mapped to columns of the same name.
    :param timestamp_field: a string for the name of an additional timestamp field to include,
                            or None for no such field.
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

    if field_to_col_mapping is not None and not isinstance(field_to_col_mapping, dict):
        raise TypeError(
            "argument 'mapping' is expected to be of dict type, " +
            "instead got " + str(field_to_col_mapping) + " of type " + type(field_to_col_mapping).__name__)
    field_to_col_mapping = _dictToMap(field_to_col_mapping)
    if have_actual_schema:
        return _produce_jtype_.avroSpec(schema, field_to_col_mapping, timestamp_field)
    return _produce_jtype_.avroSpec(schema, schema_version, field_to_col_mapping, timestamp_field)

@_passThrough
def json(include_columns = None,
         exclude_columns = None,
         mapping = None,
         nested_delim = None,
         output_nulls = False,
         timestamp_field = None):
    """
    Specify how to produce JSON data when producing a Kafka stream from a Deephaven table.

    :param include_columns: A sequence of Deephaven column names to include in the JSON output
                            as fields, or None to indicate all except the ones mentioned
                            in the exclude_columns argument.  If include_columns is not None,
                            exclude_columns should be None.
    :param exclude_columns: A sequence of Deephaven column names to omit in the JSON output
                            as fields.  If exclude_columns is not None, include_columns should be None.
    :param mapping: A dict mapping column names to JSON field names.  Any column name
                    implied by earlier arguments and not included as a key in the map
                    implies a field of the same name; if this argument is None all columns
                    will be mapped to JSON fields of the same name.
    :param nested_delim: if nested JSON fields are desired, the field separator that is used
                         for the fieldNames parameter, or None for no nesting (default).
                         For instance, if a particular column should be mapped to JSON field X
                         nested inside field Y, the corresponding field name value for the column key
                         in the mapping dict can be the string "X.Y",
                         in which case the value for nested_delim should be "."
    :param output_nulls: if False (default), do not output a field for null column values.
    :param timestamp_field: a string for the name of an additional timestamp field to include,
                            or None for no such field.
    :return:  A Kafka Key or Value spec object to use in a call to produceFromTable.
    :raises:  ValueError, TypeError or Exception if arguments provided can't be processed.
    """
    if include_columns is not None and exclude_columns is not None:
        raise Exception(
            "Both include_columns (=" + str(include_columns) +
            ") and exclude_columns (=" + str(exclude_columns) +
            " are not None, at least one of them should be None."
        )
    if mapping is not None and not isinstance(mapping, dict):
        raise TypeError(
            "argument 'mapping' is expected to be of dict type, " +
            "instead got " + str(mapping) + " of type " + type(mapping).__name__)
    exclude_columns = _seqToSet(exclude_columns)
    mapping = _dictToMap(mapping)
    return _produce_jtype_.jsonSpec(include_columns, exclude_columns, mapping, nested_delim, output_nulls, timestamp_field)

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
