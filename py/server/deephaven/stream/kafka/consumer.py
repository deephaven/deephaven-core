#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" The kafka.consumer module supports consuming a Kakfa topic as a Deephaven live table. """
import jpy
from typing import Dict, Tuple, List, Callable, Union, Optional
from warnings import warn

from deephaven import dtypes
from deephaven._wrapper import JObjectWrapper
from deephaven.column import Column
from deephaven.dherror import DHError
from deephaven.dtypes import DType
from deephaven.jcompat import j_hashmap, j_properties, j_array_list
from deephaven.table import Table, PartitionedTable

_JKafkaTools = jpy.get_type("io.deephaven.kafka.KafkaTools")
_JKafkaTools_Consume = jpy.get_type("io.deephaven.kafka.KafkaTools$Consume")
_JProtobufConsumeOptions = jpy.get_type("io.deephaven.kafka.protobuf.ProtobufConsumeOptions")
_JProtobufDescriptorParserOptions = jpy.get_type("io.deephaven.protobuf.ProtobufDescriptorParserOptions")
_JDescriptorSchemaRegistry = jpy.get_type("io.deephaven.kafka.protobuf.DescriptorSchemaRegistry")
_JDescriptorMessageClass = jpy.get_type("io.deephaven.kafka.protobuf.DescriptorMessageClass")
_JProtocol = jpy.get_type("io.deephaven.kafka.protobuf.Protocol")
_JFieldOptions = jpy.get_type("io.deephaven.protobuf.FieldOptions")
_JFieldPath = jpy.get_type("io.deephaven.protobuf.FieldPath")
_JPythonTools = jpy.get_type("io.deephaven.integrations.python.PythonTools")
ALL_PARTITIONS = _JKafkaTools.ALL_PARTITIONS

SEEK_TO_BEGINNING = _JKafkaTools.SEEK_TO_BEGINNING
""" Start consuming at the beginning of a partition. """
DONT_SEEK = _JKafkaTools.DONT_SEEK
""" Start consuming at the current position of a partition. """
SEEK_TO_END = _JKafkaTools.SEEK_TO_END
""" Start consuming at the end of a partition. """

ALL_PARTITIONS_SEEK_TO_BEGINNING = {-1: SEEK_TO_BEGINNING}
""" For all partitions, start consuming at the beginning. """
ALL_PARTITIONS_DONT_SEEK = {-1: DONT_SEEK}
""" For all partitions, start consuming at the current position."""
ALL_PARTITIONS_SEEK_TO_END = {-1: SEEK_TO_END}
""" For all partitions, start consuming at the end. """

_ALL_PARTITIONS_SEEK_TO_BEGINNING = _JKafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING
_ALL_PARTITIONS_DONT_SEEK = _JKafkaTools.ALL_PARTITIONS_DONT_SEEK
_ALL_PARTITIONS_SEEK_TO_END = _JKafkaTools.ALL_PARTITIONS_SEEK_TO_END


class KeyValueSpec(JObjectWrapper):
    j_object_type = jpy.get_type("io.deephaven.kafka.KafkaTools$Consume$KeyOrValueSpec")

    def __init__(self, j_spec: jpy.JType):
        self._j_spec = j_spec

    @property
    def j_object(self) -> jpy.JType:
        return self._j_spec


KeyValueSpec.IGNORE = KeyValueSpec(_JKafkaTools_Consume.IGNORE)
""" The spec for explicitly ignoring either key or value in a Kafka message when consuming a Kafka stream. """

KeyValueSpec.FROM_PROPERTIES = KeyValueSpec(_JKafkaTools.FROM_PROPERTIES)
""" The spec for specifying that when consuming a Kafka stream, the names for the key or value columns can be provided
in the properties as "key.column.name" or "value.column.name" in the config, and otherwise default to "key" or "value".
"""


class TableType(JObjectWrapper):
    """A factory that creates the supported Table Type for consuming Kafka."""

    j_object_type = jpy.get_type("io.deephaven.kafka.KafkaTools$TableType")

    @staticmethod
    def blink():
        """ Consume all partitions into a single interleaved blink table, which will present only newly-available rows
         to downstream operations and visualizations."""
        return TableType(TableType.j_object_type.blink())

    # TODO (https://github.com/deephaven/deephaven-core/issues/3853): Delete this method
    @staticmethod
    def stream():
        """ Deprecated synonym for "blink"."""
        warn('This function is deprecated, prefer blink', DeprecationWarning, stacklevel=2)
        return TableType.blink()

    @staticmethod
    def append():
        """ Consume all partitions into a single interleaved in-memory append-only table."""
        return TableType(TableType.j_object_type.append())

    @staticmethod
    def ring(capacity: int):
        """ Consume all partitions into a single in-memory ring table."""
        return TableType(TableType.j_object_type.ring(capacity))

    def __init__(self, j_table_type: jpy.JType):
        self._j_table_type = j_table_type

    @property
    def j_object(self) -> jpy.JType:
        return self._j_table_type

# TODO (https://github.com/deephaven/deephaven-core/issues/3853): Delete this attribute
TableType.Stream = TableType.blink()
""" Deprecated, prefer TableType.blink(). Consume all partitions into a single interleaved blink table, which will
present only newly-available rows to downstream operations and visualizations."""

# TODO (https://github.com/deephaven/deephaven-core/issues/3853): Delete this attribute
TableType.Append = TableType.append()
""" Deprecated, prefer TableType.append(). Consume all partitions into a single interleaved in-memory append-only table."""


def j_partitions(partitions):
    if partitions is None:
        partitions = ALL_PARTITIONS
    else:
        j_array = dtypes.array(dtypes.int32, partitions)
        partitions = _JKafkaTools.partitionFilterFromArray(j_array)
    return partitions


def _dict_to_j_func(dict_mapping: Dict, mapped_only: bool) -> Callable[[str], str]:
    java_map = j_hashmap(dict_mapping)
    if not mapped_only:
        return _JPythonTools.functionFromMapWithIdentityDefaults(java_map)
    return _JPythonTools.functionFromMapWithDefault(java_map, None)


def consume(
        kafka_config: Dict,
        topic: str,
        partitions: List[int] = None,
        offsets: Dict[int, int] = None,
        key_spec: KeyValueSpec = None,
        value_spec: KeyValueSpec = None,
        table_type: TableType = TableType.blink(),
) -> Table:
    """Consume from Kafka to a Deephaven table.

    Args:
        kafka_config (Dict): configuration for the associated Kafka consumer and also the resulting table.
            Once the table-specific properties are stripped, the remaining one is used to call the constructor of
            org.apache.kafka.clients.consumer.KafkaConsumer; pass any KafkaConsumer specific desired configuration here
        topic (str): the Kafka topic name
        partitions (List[int]) : a list of integer partition numbers, default is None which means all partitions
        offsets (Dict[int, int]) : a mapping between partition numbers and offset numbers, and can be one of the
            predefined ALL_PARTITIONS_SEEK_TO_BEGINNING, ALL_PARTITIONS_SEEK_TO_END or ALL_PARTITIONS_DONT_SEEK.
            The default is None which works the same as  ALL_PARTITIONS_DONT_SEEK. The offset numbers may be one
            of the predefined SEEK_TO_BEGINNING, SEEK_TO_END, or DONT_SEEK.
        key_spec (KeyValueSpec): specifies how to map the Key field in Kafka messages to Deephaven column(s).
            It can be the result of calling one of the functions: simple_spec(),avro_spec() or json_spec() in this
            module, or the predefined KeyValueSpec.IGNORE or KeyValueSpec.FROM_PROPERTIES. The default is None which
            works the same as KeyValueSpec.FROM_PROPERTIES, in which case, the kafka_config param should include values
            for dictionary keys 'deephaven.key.column.name' and 'deephaven.key.column.type', for the single resulting
            column name and type
        value_spec (KeyValueSpec): specifies how to map the Value field in Kafka messages to Deephaven column(s).
            It can be the result of calling one of the functions: simple_spec(),avro_spec() or json_spec() in this
            module, or the predefined KeyValueSpec.IGNORE or KeyValueSpec.FROM_PROPERTIES. The default is None which
            works the same as KeyValueSpec.FROM_PROPERTIES, in which case, the kafka_config param should include values
            for dictionary keys 'deephaven.value.column.name' and 'deephaven.value.column.type', for the single resulting
            column name and type
        table_type (TableType): a TableType, default is TableType.blink()

    Returns:
        a Deephaven live table that will update based on Kafka messages consumed for the given topic

    Raises:
        DHError
    """

    return _consume(kafka_config, topic, partitions, offsets, key_spec, value_spec, table_type, to_partitioned=False)


def consume_to_partitioned_table(
        kafka_config: Dict,
        topic: str,
        partitions: List[int] = None,
        offsets: Dict[int, int] = None,
        key_spec: KeyValueSpec = None,
        value_spec: KeyValueSpec = None,
        table_type: TableType = TableType.blink(),
) -> PartitionedTable:
    """Consume from Kafka to a Deephaven partitioned table.

    Args:
        kafka_config (Dict): configuration for the associated Kafka consumer and also the resulting table.
            Once the table-specific properties are stripped, the remaining one is used to call the constructor of
            org.apache.kafka.clients.consumer.KafkaConsumer; pass any KafkaConsumer specific desired configuration here
        topic (str): the Kafka topic name
        partitions (List[int]) : a list of integer partition numbers, default is None which means all partitions
        offsets (Dict[int, int]) : a mapping between partition numbers and offset numbers, and can be one of the
            predefined ALL_PARTITIONS_SEEK_TO_BEGINNING, ALL_PARTITIONS_SEEK_TO_END or ALL_PARTITIONS_DONT_SEEK.
            The default is None which works the same as  ALL_PARTITIONS_DONT_SEEK. The offset numbers may be one
            of the predefined SEEK_TO_BEGINNING, SEEK_TO_END, or DONT_SEEK.
        key_spec (KeyValueSpec): specifies how to map the Key field in Kafka messages to Deephaven column(s).
            It can be the result of calling one of the functions: simple_spec(),avro_spec() or json_spec() in this
            module, or the predefined KeyValueSpec.IGNORE or KeyValueSpec.FROM_PROPERTIES. The default is None which
            works the same as KeyValueSpec.FROM_PROPERTIES, in which case, the kafka_config param should include values
            for dictionary keys 'deephaven.key.column.name' and 'deephaven.key.column.type', for the single resulting
            column name and type
        value_spec (KeyValueSpec): specifies how to map the Value field in Kafka messages to Deephaven column(s).
            It can be the result of calling one of the functions: simple_spec(),avro_spec() or json_spec() in this
            module, or the predefined KeyValueSpec.IGNORE or KeyValueSpec.FROM_PROPERTIES. The default is None which
            works the same as KeyValueSpec.FROM_PROPERTIES, in which case, the kafka_config param should include values
            for dictionary keys 'deephaven.value.column.name' and 'deephaven.value.column.type', for the single resulting
            column name and type
        table_type (TableType): a TableType, specifying the type of the expected result's constituent tables,
            default is TableType.blink()

    Returns:
        a Deephaven live partitioned table that will update based on Kafka messages consumed for the given topic,
        the keys of this partitioned table are the partition numbers of the topic, and its constituents are tables per
        topic partition.

    Raises:
        DHError
    """

    return _consume(kafka_config, topic, partitions, offsets, key_spec, value_spec, table_type, to_partitioned=True)


def _consume(
        kafka_config: Dict,
        topic: str,
        partitions: List[int] = None,
        offsets: Dict[int, int] = None,
        key_spec: KeyValueSpec = None,
        value_spec: KeyValueSpec = None,
        table_type: TableType = TableType.blink(),
        to_partitioned: bool = False,
) -> Union[Table, PartitionedTable]:
    try:
        partitions = j_partitions(partitions)

        if offsets is None or offsets == ALL_PARTITIONS_DONT_SEEK:
            offsets = _ALL_PARTITIONS_DONT_SEEK
        elif offsets == ALL_PARTITIONS_SEEK_TO_BEGINNING:
            offsets = _ALL_PARTITIONS_SEEK_TO_BEGINNING
        elif offsets == ALL_PARTITIONS_SEEK_TO_END:
            offsets = _ALL_PARTITIONS_SEEK_TO_END
        else:
            partitions_array = jpy.array("int", list(offsets.keys()))
            offsets_array = jpy.array("long", list(offsets.values()))
            offsets = _JKafkaTools.partitionToOffsetFromParallelArrays(
                partitions_array, offsets_array
            )

        key_spec = KeyValueSpec.FROM_PROPERTIES if key_spec is None else key_spec
        value_spec = KeyValueSpec.FROM_PROPERTIES if value_spec is None else value_spec

        if key_spec is KeyValueSpec.IGNORE and value_spec is KeyValueSpec.IGNORE:
            raise ValueError("at least one argument for 'key' or 'value' must be different from KeyValueSpec.IGNORE")

        kafka_config = j_properties(kafka_config)
        if not to_partitioned:
            return Table(
                j_table=_JKafkaTools.consumeToTable(
                    kafka_config,
                    topic,
                    partitions,
                    offsets,
                    key_spec.j_object,
                    value_spec.j_object,
                    table_type.j_object,
                )
            )
        else:
            return PartitionedTable(j_partitioned_table=_JKafkaTools.consumeToPartitionedTable(
                kafka_config,
                topic,
                partitions,
                offsets,
                key_spec.j_object,
                value_spec.j_object,
                table_type.j_object,
            ))
    except Exception as e:
        raise DHError(e, "failed to consume a Kafka stream.") from e

class ProtobufProtocol(JObjectWrapper):
    """The protobuf serialization / deserialization protocol."""

    j_object_type = jpy.get_type("io.deephaven.kafka.protobuf.Protocol")

    @staticmethod
    def serdes() -> 'ProtobufProtocol':
        """The Kafka Protobuf serdes protocol. The payload's first byte is the serdes magic byte, the next 4-bytes are
        the schema ID, the next variable-sized bytes are the message indexes, followed by the normal binary encoding of
        the Protobuf data."""
        return ProtobufProtocol(ProtobufProtocol.j_object_type.serdes())

    @staticmethod
    def raw() -> 'ProtobufProtocol':
        """The raw Protobuf protocol. The full payload is the normal binary encoding of the Protobuf data."""
        return ProtobufProtocol(ProtobufProtocol.j_object_type.raw())

    def __init__(self, j_protocol: jpy.JType):
        self._j_protocol = j_protocol

    @property
    def j_object(self) -> jpy.JType:
        return self._j_protocol


def protobuf_spec(
        schema: Optional[str] = None,
        schema_version: Optional[int] = None,
        schema_message_name: Optional[str] = None,
        message_class: Optional[str] = None,
        include: Optional[List[str]] = None,
        protocol: Optional[ProtobufProtocol] = None,
) -> KeyValueSpec:
    """Creates a spec for parsing a Kafka protobuf stream into a Deephaven table. Uses the schema, schema_version, and
    schema_message_name to fetch the schema from the schema registry; or uses message_class to to get the schema from
    the classpath.

    Args:
        schema (Optional[str]): the schema subject name. When set, this will fetch the protobuf message descriptor from
            the schema registry. Either this, or message_class, must be set.
        schema_version (Optional[int]): the schema version, or None for latest, default is None. For purposes of
            reproducibility across restarts where schema changes may occur, it is advisable for callers to set this.
            This will ensure the resulting table definition will not change across restarts. This gives the caller an
            explicit opportunity to update any downstream consumers when updating schema_version if necessary.
        schema_message_name (Optional[str]): the fully-qualified protobuf message name, for example
            "com.example.MyMessage". This message's descriptor will be used as the basis for the resulting table's
            definition. If None, the first message descriptor in the protobuf schema will be used. The default is None.
            It is advisable for callers to explicitly set this.
        message_class (Optional[str]): the fully-qualified Java class name for the protobuf message on the current
            classpath, for example "com.example.MyMessage" or "com.example.OuterClass$MyMessage". When this is set, the
            schema registry will not be used. Either this, or schema, must be set.
        include (Optional[List[str]]): the '/' separated paths to include. The final path may be a '*' to additionally
            match everything that starts with path. For example, include=["/foo/bar"] will include the field path
            name paths [], ["foo"], and ["foo", "bar"]. include=["/foo/bar/*"] will additionally include any field path
            name paths that start with ["foo", "bar"]:  ["foo", "bar", "baz"],  ["foo", "bar", "baz", "zap"], etc. When
            multiple includes are specified, the fields will be included when any of the components matches. Default is
            None, which includes all paths.
        protocol (Optional[ProtobufProtocol]): the wire protocol for this payload. When schema is set,
            ProtobufProtocol.serdes() will be used by default. When message_class is set, ProtobufProtocol.raw() will
            be used by default.
    Returns:
        a KeyValueSpec
    """
    parser_options_builder = _JProtobufDescriptorParserOptions.builder()
    if include is not None:
        parser_options_builder.fieldOptions(
            _JFieldOptions.includeIf(
                _JFieldPath.anyMatches(j_array_list(include))
            )
        )
    pb_consume_builder = (
        _JProtobufConsumeOptions.builder()
        .parserOptions(parser_options_builder.build())
    )
    if message_class:
        if schema or schema_version or schema_message_name:
            raise DHError("Must only set schema information, or message_class, but not both.")
        pb_consume_builder.descriptorProvider(_JDescriptorMessageClass.of(jpy.get_type(message_class).jclass))
    elif schema:
        dsr = _JDescriptorSchemaRegistry.builder().subject(schema)
        if schema_version:
            dsr.version(schema_version)
        if schema_message_name:
            dsr.messageName(schema_message_name)
        pb_consume_builder.descriptorProvider(dsr.build())
    else:
        raise DHError("Must set schema or message_class")
    if protocol:
        pb_consume_builder.protocol(protocol.j_object)
    return KeyValueSpec(
        j_spec=_JKafkaTools_Consume.protobufSpec(pb_consume_builder.build())
    )


def avro_spec(
        schema: str,
        schema_version: str = "latest",
        mapping: Dict[str, str] = None,
        mapped_only: bool = False,
) -> KeyValueSpec:
    """Creates a spec for how to use an Avro schema when consuming a Kafka stream to a Deephaven table.

    Args:
        schema (str): Either a JSON encoded Avro schema definition string, or
            the name for a schema registered in a Confluent compatible Schema Server.
            If the name for a schema in Schema Server, the associated
            'kafka_config' parameter in the call to consume() should include the key 'schema.registry.url' with
            the value of the Schema Server URL for fetching the schema definition
        schema_version (str): the schema version to fetch from schema service, default is 'latest'
        mapping (Dict[str, str]): a mapping from Avro field name to Deephaven table column name; the fields specified in
            the mapping will have their column names defined by it; if 'mapped_only' parameter is False, any other fields
            not mentioned in the mapping will use the same Avro field name for Deephaven table column; otherwise, these
            unmapped fields will be ignored and will not be present in the resulting table. default is None
        mapped_only (bool): whether to ignore Avro fields not present in the 'mapping' argument, default is False

    Returns:
        a KeyValueSpec

    Raises:
        DHError
    """
    try:
        if mapping is not None:
            mapping = _dict_to_j_func(mapping, mapped_only)

        if schema.strip().startswith("{"):
            jschema = _JKafkaTools.getAvroSchema(schema);
            if mapping:
                return KeyValueSpec(
                    j_spec=_JKafkaTools_Consume.avroSpec(jschema, mapping)
                )
            else:
                return KeyValueSpec(
                    j_spec=_JKafkaTools_Consume.avroSpec(jschema)
                )

        else:
            if mapping:
                return KeyValueSpec(
                    j_spec=_JKafkaTools_Consume.avroSpec(schema, schema_version, mapping)
                )
            else:
                return KeyValueSpec(
                    j_spec=_JKafkaTools_Consume.avroSpec(schema, schema_version)
                )
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec") from e


def json_spec(col_defs: Union[Dict[str, DType], List[Tuple[str, DType]]], mapping: Dict = None) -> KeyValueSpec:
    """Creates a spec for how to use JSON data when consuming a Kafka stream to a Deephaven table.

    Args:
        col_defs (Union[Dict[str, DType], List[Tuple[str, DType]]): the column definitions, either a map of column
            names and Deephaven types, or a list of tuples with two elements, a string for column name and a Deephaven
            type for column data type.
        mapping (Dict): a dict mapping JSON fields to column names defined in the col_defs
            argument.  Fields starting with a '/' character are interpreted as a JSON Pointer (see RFC 6901,
            ISSN: 2070-1721 for details, essentially nested fields are represented like "/parent/nested").
            Fields not starting with a '/' character are interpreted as toplevel field names.
            If the mapping argument is not present or None, a 1:1 mapping between JSON fields and Deephaven
            table column names is assumed.

    Returns:
        a KeyValueSpec

    Raises:
        DHError
    """
    try:
        if isinstance(col_defs, dict):
            col_defs = [Column(k, v).j_column_definition for k, v in col_defs.items()]
        else:
            col_defs = [Column(*t).j_column_definition for t in col_defs]

        if mapping is None:
            return KeyValueSpec(j_spec=_JKafkaTools_Consume.jsonSpec(col_defs))
        mapping = j_hashmap(mapping)
        return KeyValueSpec(j_spec=_JKafkaTools_Consume.jsonSpec(col_defs, mapping))
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec") from e


def simple_spec(col_name: str, data_type: DType = None) -> KeyValueSpec:
    """Creates a spec that defines a single column to receive the key or value of a Kafka message when consuming a
    Kafka stream to a Deephaven table.

    Args:
        col_name (str): the Deephaven column name
        data_type (DType): the column data type

    Returns:
        a KeyValueSpec

    Raises:
        DHError
    """
    try:
        if data_type is None:
            return KeyValueSpec(j_spec=_JKafkaTools_Consume.simpleSpec(col_name))
        return KeyValueSpec(
            j_spec=_JKafkaTools_Consume.simpleSpec(col_name, data_type.qst_type.clazz())
        )
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec") from e
