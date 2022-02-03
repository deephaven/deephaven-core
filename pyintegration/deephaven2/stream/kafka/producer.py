#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" The kafka.producer module allows publishing Deephavne tables to Kafka streams. """
import jpy

from deephaven2 import DHError, dtypes

_JKafkaTools = jpy.get_type("io.deephaven.kafka.KafkaTools")
_JAvroSchema = jpy.get_type("org.apache.avro.Schema")
_JKafkaTools_Produce = jpy.get_type("io.deephaven.kafka.KafkaTools$Produce")
IGNORE = getattr(_JKafkaTools_Produce, 'IGNORE')


def produce(
        table,
        kafka_config: dict,
        topic: str,
        key,
        value,
        last_by_key_columns: bool = False
):
    """ Produce to Kafka from a Deephaven table.

    Args:
        table: a Deephaven table used as a source of rows to publish to Kafka.
        kafka_config: dictionary with properties to configure the associated kafka producer.
        topic: the topic name
        key: a specification for how to map table column(s) to the Key field in produced
            Kafka messages.  This should be the result of calling one of the methods
            simple, avro or json in this module, or the constant _IGNORE.
        value: a specification for how to map table column(s) to the Value field in produced
            Kafka messages.  This should be the result of calling one of the methods
            simple, avro or json in this module, or the constant _IGNORE.
        last_by_key_columns:  Whether to publish only the last record for each unique key.
            Ignored if key is _IGNORE.  If key is not _IGNORE and last_by_key_columns is false,
            it is expected that table updates will not produce any row shifts; that is, the publisher
            expects keyed tables to be streams, add-only, or aggregated.

    Returns:
        a callback that, when invoked, stops publishing and cleans up subscriptions and resources.
        Users should hold to this callback to ensure liveness for publishing for as long as this
        publishing is desired, and once not desired anymore they should invoke it.

    Raises:
        ValueError, TypeError, DHError
    """

    if not isinstance(topic, str):
        raise ValueError("argument 'topic' has to be of str type, instead got " + topic)

    if key is None:
        raise ValueError("argument 'key' is None")
    if value is None:
        raise ValueError("argument 'value' is None")
    if key is IGNORE and value is IGNORE:
        raise ValueError(
            "at least one argument for 'key' or 'value' must be different from _IGNORE")

    kafka_config = dtypes.Properties(kafka_config)
    try:
        runnable = _JKafkaTools.produceFromTable(table.j_table, kafka_config, topic, key, value, last_by_key_columns)
    except Exception as e:
        raise DHError(e, "failed to start producing Kafka messages.") from e

    def cleanup():
        runnable.run()

    return cleanup


def avro(schema,
         schema_version: str = None,
         field_to_col_mapping=None,
         timestamp_field=None,
         include_only_columns=None,
         exclude_columns=None,
         publish_schema: bool = False,
         schema_namespace: str = None,
         column_properties=None
         ):
    """ Specify an Avro schema to use when producing a Kafka stream from a Deephaven table.

    Args:
        schema:  either an Avro schema object or a string specifying a schema name for a schema
            registered in a Confluent compatible Schema Server.  When the latter is provided, the
            associated kafka_config dict in the call to consumeToTable should include the key
            'schema.registry.url' with the associated value of the Schema Server URL for fetching the schema
            definition.
        schema_version:  if a string schema name is provided, the version to fetch from schema
            service; if not specified, a default of 'latest' is assumed.
        field_to_col_mapping: a dict mapping field names in the schema to column names in the Deephaven table.
            Any fields in the schema not present in the dict as keys are mapped to columns of the same name.
            If this argument is None, all schema fields are mapped to columns of the same name.
        timestamp_field: a string for the name of an additional timestamp field to include,
            or None for no such field.
        include_only_columns: If not None, a sequence of column names in the source table to include
           in the generated output.   Only one of include_only_columns and exclude_columns can be different from None.
           Defaults to None.
        exclude_columns: If not None, a sequence of column names to exclude from the generated output (every other column
           will be included).   Only one of include_only_columns and exclude_columns can be different from None.
           Defaults to None.
        publish_schema: If True, publish the given schema name to Schema Registry Server, according to an Avro schema
            generated from the table definition, for the columns and fields implied by field_to_col_mapping,
            include_only_columns, and exclude_columns.  When true, if a schema_version is provided and the resulting
            version after publishing does not match, an exception results.
        schema_namespace: When publish_schema is True, the namespace for the generated schema to be restered in
            Schema Registry Server.
        column_properties: When publish_schema is True, a dict containing string properties for columns specifying
            string properties implying particular Avro type mappings for them.   In particular, column X of BigDecimal
            type should specify string properties 'x.precision' and 'x.scale'.

    Returns:
        a Kafka Key or Value spec object to use in a call to produceFromTable.

    Raises:
        ValueError, TypeError, DHError
    """
    if isinstance(schema, str):
        have_actual_schema = False
        if schema_version is None:
            schema_version = "latest"
        elif not isinstance(schema_version, str):
            raise TypeError(
                "argument 'schema_version' should be of str type, instead got " +
                str(schema_version) + " of type " + type(schema_version).__name__)
    elif isinstance(schema, _JAvroSchema):
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
    if column_properties is not None and not isinstance(column_properties, dict):
        raise TypeError(
            "argument 'column_properties' is excpected to be of dict type, " +
            "instead got " + str(column_properties) + " of type " + type(column_properties).__name__)

    field_to_col_mapping = dtypes.HashMap(field_to_col_mapping)
    column_properties = dtypes.Properties(column_properties)
    include_only_columns = dtypes.HashSet(include_only_columns)
    include_only_columns = _JKafkaTools.predicateFromSet(include_only_columns)
    exclude_columns = dtypes.HashSet(exclude_columns)
    exclude_columns = _JKafkaTools.predicateFromSet(exclude_columns)
    publish_schema = bool(publish_schema)

    try:
        if have_actual_schema:
            return _JKafkaTools_Produce.avroSpec(schema, field_to_col_mapping, timestamp_field,
                                                 include_only_columns, exclude_columns, publish_schema,
                                                 schema_namespace, column_properties)
        return _JKafkaTools_Produce.avroSpec(schema, schema_version, field_to_col_mapping, timestamp_field,
                                             include_only_columns, exclude_columns, publish_schema,
                                             schema_namespace, column_properties)
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec.") from e


def json(include_columns=None,
         exclude_columns=None,
         mapping=None,
         nested_delim=None,
         output_nulls=False,
         timestamp_field=None):
    """ Specify how to produce JSON data when producing a Kafka stream from a Deephaven table.

    Args:
        include_columns: a sequence of Deephaven column names to include in the JSON output
            as fields, or None to indicate all except the ones mentioned in the exclude_columns argument.
            If include_columns is not None, exclude_columns should be None.
        exclude_columns: a sequence of Deephaven column names to omit in the JSON output as fields.
            If exclude_columns is not None, include_columns should be None.
        mapping: a dict mapping column names to JSON field names.  Any column name implied by earlier arguments
            and not included as a key in the map implies a field of the same name; if this argument is None all
            columns will be mapped to JSON fields of the same name.
        nested_delim: if nested JSON fields are desired, the field separator that is used for the fieldNames
            parameter, or None for no nesting (default). For instance, if a particular column should be mapped
            to JSON field X nested inside field Y, the corresponding field name value for the column key
            in the mapping dict can be the string "X.Y", in which case the value for nested_delim should be "."
        output_nulls: if False (default), do not output a field for null column values.
        timestamp_field: a string for the name of an additional timestamp field to include, or None for no such field.

    Returns:
        a Kafka Key or Value spec object to use in a call to produceFromTable.

    Raises:
        ValueError, TypeError, DHError
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
    exclude_columns = dtypes.HashSet(exclude_columns)
    mapping = dtypes.HashMap(mapping)
    try:
        return _JKafkaTools_Produce.jsonSpec(include_columns, exclude_columns, mapping, nested_delim, output_nulls,
                                             timestamp_field)
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec.") from e


def simple(column_name: str):
    """  Specify a single column when producing to a Kafka Key or Value field.

    Args:
        column_name:  a string specifying the Deephaven column name to use.

    Returns:
        a Kafka Key or Value spec object to use in a call to produceFromTable.

    Raises:
        TypeError, DHError
    """
    if not isinstance(column_name, str):
        raise TypeError(
            "'column_name' argument needs to be of str type, instead got " + str(column_name))

    try:
        return _JKafkaTools_Produce.simpleSpec(column_name)
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec.") from e
