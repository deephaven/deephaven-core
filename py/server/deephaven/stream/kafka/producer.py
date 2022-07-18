#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" The kafka.producer module supports publishing Deephaven tables to Kafka streams. """
from typing import Dict, Callable, List

import jpy

from deephaven import DHError
from deephaven.jcompat import j_hashmap, j_hashset, j_properties
from deephaven._wrapper import JObjectWrapper
from deephaven.table import Table

_JKafkaTools = jpy.get_type("io.deephaven.kafka.KafkaTools")
_JAvroSchema = jpy.get_type("org.apache.avro.Schema")
_JKafkaTools_Produce = jpy.get_type("io.deephaven.kafka.KafkaTools$Produce")


class KeyValueSpec(JObjectWrapper):
    j_object_type = jpy.get_type("io.deephaven.kafka.KafkaTools$Produce$KeyOrValueSpec")

    def __init__(self, j_spec: jpy.JType):
        self._j_spec = j_spec

    @property
    def j_object(self) -> jpy.JType:
        return self._j_spec


KeyValueSpec.IGNORE = KeyValueSpec(_JKafkaTools_Produce.IGNORE)


def produce(
    table: Table,
    kafka_config: Dict,
    topic: str,
    key_spec: KeyValueSpec,
    value_spec: KeyValueSpec,
    last_by_key_columns: bool = False,
) -> Callable[[], None]:
    """Produce to Kafka from a Deephaven table.

    Args:
        table (Table): the source table to publish to Kafka
        kafka_config (Dict): configuration for the associated kafka producer
        topic (str): the topic name
        key_spec (KeyValueSpec): specifies how to map table column(s) to the Key field in produced Kafka messages.
            This should be the result of calling one of the functions simple_spec(), avro_spec() or json_spec() in this
            module, or the constant KeyValueSpec.IGNORE
        value_spec (KeyValueSpec): specifies how to map table column(s) to the Value field in produced Kafka messages.
            This should be the result of calling one of the functions simple_spec(), avro_spec() or json_spec() in this,
            or the constant KeyValueSpec.IGNORE
        last_by_key_columns (bool): whether to publish only the last record for each unique key, Ignored if key_spec is
            KeyValueSpec.IGNORE. Otherwise, if last_by_key_columns is true this method will internally perform a last_by
            aggregation on table grouped by the input columns of key_spec and publish to Kafka from the result.

    Returns:
        a callback that, when invoked, stops publishing and cleans up subscriptions and resources.
        Users should hold to this callback to ensure liveness for publishing for as long as this
        publishing is desired, and once not desired anymore they should invoke it

    Raises:
        DHError
    """
    try:
        if key_spec is KeyValueSpec.IGNORE and value_spec is KeyValueSpec.IGNORE:
            raise ValueError(
                "at least one argument for 'key_spec' or 'value_spec' must be different from KeyValueSpec.IGNORE"
            )

        kafka_config = j_properties(kafka_config)
        runnable = _JKafkaTools.produceFromTable(
            table.j_table,
            kafka_config,
            topic,
            key_spec.j_object,
            value_spec.j_object,
            last_by_key_columns,
        )

        def cleanup():
            try:
                runnable.run()
            except Exception as ex:
                raise DHError(ex, "failed to stop publishing to Kafka and the clean-up.") from ex

        return cleanup
    except Exception as e:
        raise DHError(e, "failed to start producing Kafka messages.") from e


def avro_spec(
    schema: str,
    schema_version: str = "latest",
    field_to_col_mapping: Dict[str, str] = None,
    timestamp_field: str = None,
    include_only_columns: List[str] = None,
    exclude_columns: List[str] = None,
    publish_schema: bool = False,
    schema_namespace: str = None,
    column_properties: Dict[str, str] = None,
) -> KeyValueSpec:
    """Creates a spec for how to use an Avro schema to produce a Kafka stream from a Deephaven table.

    Args:
        schema (str):  the name for a schema registered in a Confluent compatible Schema Server. The associated
            'kafka_config' parameter in the call to produce() should include the key 'schema.registry.url' with
            the value of the Schema Server URL for fetching the schema definition
        schema_version (str): the schema version to fetch from schema service, default is 'latest'
        field_to_col_mapping (Dict[str, str]): a mapping from Avro field names in the schema to column names in
            the Deephaven table. Any fields in the schema not present in the dict as keys are mapped to columns of the
            same name. The default is None, meaning all schema fields are mapped to columns of the same name.
        timestamp_field (str): the name of an extra timestamp field to be included in the produced Kafka message body,
            it is used mostly for debugging slowdowns,  default is None.
        include_only_columns (List[str]): the list of column names in the source table to include in the generated
            output, default is None. When not None, the 'exclude_columns' parameter must be None
        exclude_columns (List[str]):  the list of column names to exclude from the generated output (every other column
            will be included), default is None. When not None, the 'include_only_columns' must be None
        publish_schema (bool): when True, publish the given schema name to Schema Registry Server, according to an Avro
            schema generated from the table definition, for the columns and fields implied by field_to_col_mapping,
            include_only_columns, and exclude_columns; if a schema_version is provided and the resulting version after
            publishing does not match, an exception results. The default is False.
        schema_namespace (str): when 'publish_schema' is True, the namespace for the generated schema to be registered
            in the Schema Registry Server.
        column_properties (Dict[str, str]): when 'publish_schema' is True, specifies the properties of the columns
            implying particular Avro type mappings for them. In particular, column X of BigDecimal type should specify
            properties 'x.precision' and 'x.scale'.

    Returns:
        a KeyValueSpec

    Raises:
        DHError
    """
    try:
        field_to_col_mapping = j_hashmap(field_to_col_mapping)
        column_properties = j_properties(column_properties)
        include_only_columns = j_hashset(include_only_columns)
        include_only_columns = _JKafkaTools.predicateFromSet(include_only_columns)
        exclude_columns = j_hashset(exclude_columns)
        exclude_columns = _JKafkaTools.predicateFromSet(exclude_columns)

        return KeyValueSpec(
            _JKafkaTools_Produce.avroSpec(
                schema,
                schema_version,
                field_to_col_mapping,
                timestamp_field,
                include_only_columns,
                exclude_columns,
                publish_schema,
                schema_namespace,
                column_properties,
            )
        )
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec.") from e


def json_spec(
    include_columns: List[str] = None,
    exclude_columns: List[str] = None,
    mapping: Dict[str, str] = None,
    nested_delim: str = None,
    output_nulls: bool = False,
    timestamp_field: str = None,
) -> KeyValueSpec:
    """Creates a spec for how to generate JSON data when producing a Kafka stream from a Deephaven table.

    Because JSON is a nested structure, a Deephaven column can be specified to map to a top level JSON field or
    a field nested inside another JSON object many levels deep, e.g. X.Y.Z.field. The parameter 'nested_delim' controls
    how a JSON nested field name should be delimited in the mapping.

    Args:
        include_columns (List[str]): the list of Deephaven column names to include in the JSON output as fields,
            default is None, meaning all except the ones mentioned in the 'exclude_columns' argument . If not None,
            the 'exclude_columns' must be None.
        exclude_columns (List[str]): the list of Deephaven column names to omit in the JSON output as fields, default
            is None, meaning no column is omitted. If not None, include_columns must be None.
        mapping (Dict[str, str]): a mapping from column names to JSON field names.  Any column name implied by earlier
            arguments and not included as a key in the map implies a field of the same name. default is None,
            meaning all columns will be mapped to JSON fields of the same name.
        nested_delim (str): if nested JSON fields are desired, the field separator that is used for the field names
            parameter, or None for no nesting (default). For instance, if a particular column should be mapped
            to JSON field X nested inside field Y, the corresponding field name value for the column key
            in the mapping dict can be the string "X.Y", in which case the value for nested_delim should be "."
        output_nulls (bool): when False (default), do not output a field for null column values
        timestamp_field (str): the name of an extra timestamp field to be included in the produced Kafka message body,
            it is used mostly for debugging slowdowns,  default is None.

    Returns:
        a KeyValueSpec

    Raises:
        DHError
    """
    try:
        if include_columns is not None and exclude_columns is not None:
            raise ValueError("One of include_columns and exclude_columns must be None.")
        exclude_columns = j_hashset(exclude_columns)
        mapping = j_hashmap(mapping)
        return KeyValueSpec(
            _JKafkaTools_Produce.jsonSpec(
                include_columns,
                exclude_columns,
                mapping,
                nested_delim,
                output_nulls,
                timestamp_field,
            )
        )
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec.") from e


def simple_spec(col_name: str) -> KeyValueSpec:
    """Creates a spec that defines a single column to be published as either the key or value of a Kafka message when
    producing a Kafka stream from a Deephaven table.

    Args:
        col_name (str): the Deephaven column name

    Returns:
        a KeyValueSpec

    Raises:
        DHError
    """
    try:
        return KeyValueSpec(_JKafkaTools_Produce.simpleSpec(col_name))
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec.") from e
