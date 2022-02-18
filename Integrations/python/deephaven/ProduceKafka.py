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

    global _java_type_, _avro_schema_jtype_, _produce_jtype_, IGNORE
    if _java_type_ is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_ = jpy.get_type("io.deephaven.kafka.KafkaTools")
        _stream_table_tools_ = jpy.get_type("io.deephaven.engine.table.impl.StreamTableTools")
        _avro_schema_jtype_ = jpy.get_type("org.apache.avro.Schema")
        _produce_jtype_ = jpy.get_type("io.deephaven.kafka.KafkaTools$Produce")
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
        kafka_config: dict,
        topic: str,
        key,
        value,
        last_by_key_columns: bool = False
):
    """
    Produce a Kafka stream from a Deephaven table.

    Note that ``table`` must only change in ways that are meaningful when turned into a stream of events over Kafka.

    Two primary use cases are considered:

    **A stream of changes (puts and removes) to a key-value data set**
      In order to handle this efficiently and allow for correct reconstruction of the state at a consumer, it is assumed
      that the input data is the result of a Deephaven aggregation, e.g. agg_all_by, agg_by, or last_by. This means
      that key columns (as specified by ``key``) must not be modified, and no rows should be shifted if there
      are any key columns. Note that specifying ``last_by_key_columns`` as ``true`` can make it easy to satisfy this
      constraint if the input data is not already aggregated.

    **A stream of independent log records**
      In this case, the input table should either be a stream table or should only ever add rows.

    If other use cases are identified, a publication mode or extensible listener framework may be introduced at a later
    date.

    :param table: a Deephaven table used as a source of rows to publish to Kafka.
    :param kafka_config: Dictionary with properties to configure the associated kafka producer.
    :param topic: The topic name
    :param key: A specification for how to map table column(s) to the Key field in produced
           Kafka messages.  This should be the result of calling one of the methods simple, avro or json in this module,
           or the constant IGNORE. The resulting key serializer must map each input tuple to a unique output key.
    :param value: A specification for how to map table column(s) to the Value field in produced
           Kafka messages.  This should be the result of calling one of the methods
           simple, avro or json in this module, or the constant IGNORE.
    :param last_by_key_columns:  Whether to publish only the last record for each unique key.
           Ignored if key is IGNORE.  Otherwise, if last_by_key_columns is true this method will internally perform a
           last_by aggregation on table grouped by the input columns of key and publish to Kafka from the result.
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
def avro(
        schema,
        schema_version: str = None,
        field_to_col_mapping=None,
        timestamp_field: str = None,
        include_only_columns=None,
        exclude_columns=None,
        publish_schema: bool = False,
        schema_namespace: str = None,
        column_properties=None):
    """
    Specify an Avro schema to use when producing a Kafka stream from a Deephaven table.

    :param schema:  Either an Avro schema object or a string specifying a schema name for a schema
       registered in a Confluent compatible Schema Registry Server.  When the latter is provided, the
       associated kafka_config dict in the call to consumeToTable should include the key
       'schema.registry.url' with the associated value of the Schema Registry Server URL for fetching the schema
       definition.
    :param schema_version:  If a string schema name is provided, the version to fetch from schema
       service; if not specified, a default of 'latest' is assumed.
    :param field_to_col_mapping: A dict mapping field names in the schema to column names in the Deephaven table.
       Any fields in the schema not present in the dict as keys are mapped to columns of the same name (except for any columns
       ignored via exclude_columns).
       If this argument is None, all schema fields are mapped to columns of the same name (except for any columns
       ignored via exclude_columns).
    :param timestamp_field: a string for the name of an additional timestamp field to include,
                            or None for no such field.
    :param include_only_columns If not None, a sequence of column names in the source table to include
           in the generated output.   Only one of include_only_columns and exclude_columns can be different from None.
           Defaults to None.
    :param exclude_columns If not None, a sequence of column names to exclude from the generated output (every other column
           will be included).   Only one of include_only_columns and exclude_columns can be different from None.
           Defaults to None.
    :param publish_schema  If True, publish the given schema name to Schema Registry Server, according to an Avro schema
           generated from the table definition, for the columns and fields implied by field_to_col_mapping, include_only_columns,
           and exclude_columns.  When true, if a schema_version is provided and the resulting version after publishing does not match,
           an exception results.
    :param schema_namespace  When publish_schema is True, the namespace for the generated schema to be restered in Schema Registry Server.
    :param column_properties  When publish_schema is True, a dict containing string properties for columns specifying string properties
            implying particular Avro type mappings for them.   In particular, column X of BigDecimal type should specify string properties
            'x.precision' and 'x.scale'.
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
            "argument 'field_to_col_mapping' is expected to be of dict type, " +
            "instead got " + str(field_to_col_mapping) + " of type " + type(field_to_col_mapping).__name__)
    if column_properties is not None and not isinstance(column_properties, dict):
        raise TypeError(
            "argument 'column_properties' is excpected to be of dict type, " +
            "instead got " + str(column_properties) + " of type " + type(column_properties).__name__)
    field_to_col_mapping = _dictToMap(field_to_col_mapping)
    column_properties = _dictToProperties(column_properties)
    include_only_columns = _seqToSet(include_only_columns)
    include_only_columns = _java_type_.predicateFromSet(include_only_columns)
    exclude_columns = _seqToSet(exclude_columns)
    exclude_columns = _java_type_.predicateFromSet(exclude_columns)
    publish_schema = bool(publish_schema)
    if have_actual_schema:
        return _produce_jtype_.avroSpec(
            schema, field_to_col_mapping, timestamp_field,
            include_only_columns, exclude_columns, publish_schema,
            schema_namespace, column_properties)
    return _produce_jtype_.avroSpec(
        schema, schema_version, field_to_col_mapping, timestamp_field,
        include_only_columns, exclude_columns, publish_schema,
        schema_namespace, column_properties)


@_passThrough
def json(include_columns=None,
         exclude_columns=None,
         mapping=None,
         nested_delim=None,
         output_nulls=False,
         timestamp_field=None):
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
    exclude_columns = _java_type_.predicateFromSet(exclude_columns)
    mapping = _dictToMap(mapping)
    return _produce_jtype_.jsonSpec(include_columns, exclude_columns, mapping, nested_delim, output_nulls,
                                    timestamp_field)


@_passThrough
def simple(column_name: str):
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
