#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" The kafka.consumer module supports consuming a Kakfa topic as a Deephaven live table. """
from enum import Enum
from typing import Dict, Tuple, List, Callable

import jpy

from deephaven2 import dtypes
from deephaven2._jcompat import j_hashmap, j_properties
from deephaven2._wrapper_abc import JObjectWrapper
from deephaven2.column import Column
from deephaven2.dherror import DHError
from deephaven2.dtypes import DType
from deephaven2.table import Table

_JKafkaTools = jpy.get_type("io.deephaven.kafka.KafkaTools")
_JKafkaTools_Consume = jpy.get_type("io.deephaven.kafka.KafkaTools$Consume")
_JPythonTools = jpy.get_type("io.deephaven.integrations.python.PythonTools")
_JTableType = jpy.get_type("io.deephaven.kafka.KafkaTools$TableType")
_ALL_PARTITIONS = _JKafkaTools.ALL_PARTITIONS


class TableType(Enum):
    """ A Enum that defines the supported Table Type for consuming Kafka. """
    Stream = _JTableType.Stream
    """ Consume all partitions into a single interleaved stream table, which will present only newly-available rows
     to downstream operations and visualizations."""
    Append = _JTableType.Append
    """ Consume all partitions into a single interleaved in-memory append-only table."""
    StreamMap = _JTableType.StreamMap
    """ Similar to Stream, but each partition is mapped to a distinct stream table."""
    AppendMap = _JTableType.AppendMap
    """ Similar to Append, but each partition is mapped to a distinct in-memory append-only table. """


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
    _j_spec: jpy.JType

    def __init__(self, j_spec):
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


def _dict_to_j_func(dict_mapping: Dict, mapped_only: bool) -> Callable[[str], str]:
    java_map = j_hashmap(dict_mapping)
    if not mapped_only:
        return _JPythonTools.functionFromMapWithIdentityDefaults(java_map)
    return _JPythonTools.functionFromMapWithDefault(java_map, None)


def _build_column_definitions(ts: List[Tuple[str, DType]]) -> List[Column]:
    """ Converts a list of two-element tuples in the form of (name, DType) to a list of Columns. """
    cols = []
    for t in ts:
        cols.append(Column(*t))
    return cols


def consume(kafka_config: Dict, topic: str, partitions: List[int] = None, offsets: Dict[int, int] = None,
            key_spec: KeyValueSpec = None, value_spec: KeyValueSpec = None,
            table_type: TableType = TableType.Stream) -> Table:
    """ Consume from Kafka to a Deephaven table.

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
            for dictionary keys 'deephaven.key.column.name' and 'deephaven.key.column.type', for the single resulting
            column name and type
        table_type (TableType): a TableType enum, default is TableType.Stream

    Returns:
        a Deephaven live table that will update based on Kafka messages consumed for the given topic

    Raises:
        DHError
    """

    try:
        if partitions is None:
            partitions = _ALL_PARTITIONS
        else:
            j_array = dtypes.array(dtypes.int32, partitions)
            partitions = _JKafkaTools.partitionFilterFromArray(j_array)

        if offsets is None or offsets == ALL_PARTITIONS_DONT_SEEK:
            offsets = _ALL_PARTITIONS_DONT_SEEK
        elif offsets == ALL_PARTITIONS_SEEK_TO_BEGINNING:
            offsets = _ALL_PARTITIONS_SEEK_TO_BEGINNING
        elif offsets == ALL_PARTITIONS_SEEK_TO_END:
            offsets = _ALL_PARTITIONS_SEEK_TO_END
        else:
            partitions_array = jpy.array('int', list(offsets.keys()))
            offsets_array = jpy.array('long', list(offsets.values()))
            offsets = _JKafkaTools.partitionToOffsetFromParallelArrays(partitions_array, offsets_array)

        key_spec = KeyValueSpec.FROM_PROPERTIES if key_spec is None else key_spec
        value_spec = KeyValueSpec.FROM_PROPERTIES if value_spec is None else value_spec

        if key_spec is KeyValueSpec.IGNORE and value_spec is KeyValueSpec.IGNORE:
            raise ValueError(
                "at least one argument for 'key' or 'value' must be different from KeyValueSpec.IGNORE")

        kafka_config = j_properties(kafka_config)
        return Table(j_table=_JKafkaTools.consumeToTable(kafka_config, topic, partitions, offsets, key_spec.j_object,
                                                         value_spec.j_object,
                                                         table_type.value))
    except Exception as e:
        raise DHError(e, "failed to consume a Kafka stream.") from e


def avro_spec(schema: str, schema_version: str = "latest", mapping: Dict[str, str] = None,
              mapped_only: bool = False) -> KeyValueSpec:
    """ Creates a spec for how to use an Avro schema when consuming a Kafka stream to a Deephaven table.

    Args:
        schema (str): the name for a schema registered in a Confluent compatible Schema Server. The associated
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

        if mapping:
            return KeyValueSpec(j_spec=_JKafkaTools_Consume.avroSpec(schema, schema_version, mapping))
        else:
            return KeyValueSpec(j_spec=_JKafkaTools_Consume.avroSpec(schema, schema_version))
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec") from e


def json_spec(col_defs: List[Tuple[str, DType]], mapping: Dict = None) -> KeyValueSpec:
    """ Creates a spec for how to use JSON data when consuming a Kafka stream to a Deephaven table.

    Args:
        col_defs (List[Tuple[str, DType]]):  a list of tuples specifying names and types for columns to be
            created on the resulting Deephaven table.  Tuples contain two elements, a string for column name
            and a Deephaven type for column data type.
        mapping (Dict): a map from JSON field names to column names defined in the col_defs argument, default is None,
            meaning a 1:1 mapping between JSON fields and Deephaven table column names

    Returns:
        a KeyValueSpec

    Raises:
        DHError
    """
    try:
        col_defs = [c.j_column_definition for c in _build_column_definitions(col_defs)]
        if mapping is None:
            return KeyValueSpec(j_spec=_JKafkaTools_Consume.jsonSpec(col_defs))
        mapping = j_hashmap(mapping)
        return KeyValueSpec(j_spec=_JKafkaTools_Consume.jsonSpec(col_defs, mapping))
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec") from e


def simple_spec(col_name: str, data_type: DType = None) -> KeyValueSpec:
    """ Creates a spec that defines a single column to receive the key or value of a Kafka message when consuming a
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
        return KeyValueSpec(j_spec=_JKafkaTools_Consume.simpleSpec(col_name, data_type.qst_type.clazz()))
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec") from e
