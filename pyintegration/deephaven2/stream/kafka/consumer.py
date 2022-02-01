#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" The Kafka consumer module. """
import collections
from typing import Any, Dict

import jpy

from deephaven2 import dtypes
from deephaven2.column import Column
from deephaven2.dherror import DHError
from deephaven2.dtypes import DType
from deephaven2.table import Table

_JKafkaTools = jpy.get_type("io.deephaven.kafka.KafkaTools")
_JStreamTableTools = jpy.get_type("io.deephaven.engine.table.impl.StreamTableTools")
_JAvroSchema = jpy.get_type("org.apache.avro.Schema")
_JKafkaTools_Consume = jpy.get_type("io.deephaven.kafka.KafkaTools$Consume")
_JPythonTools = jpy.get_type("io.deephaven.integrations.python.PythonTools")

SEEK_TO_BEGINNING = getattr(_JKafkaTools, 'SEEK_TO_BEGINNING')
DONT_SEEK = getattr(_JKafkaTools, 'DONT_SEEK')
SEEK_TO_END = getattr(_JKafkaTools, 'SEEK_TO_END')
FROM_PROPERTIES = getattr(_JKafkaTools, 'FROM_PROPERTIES')
IGNORE = getattr(_JKafkaTools_Consume, 'IGNORE')
ALL_PARTITIONS = getattr(_JKafkaTools, 'ALL_PARTITIONS')
ALL_PARTITIONS_SEEK_TO_BEGINNING = getattr(_JKafkaTools, 'ALL_PARTITIONS_SEEK_TO_BEGINNING')
ALL_PARTITIONS_DONT_SEEK = getattr(_JKafkaTools, 'ALL_PARTITIONS_DONT_SEEK')
ALL_PARTITIONS_SEEK_TO_END = getattr(_JKafkaTools, 'ALL_PARTITIONS_SEEK_TO_END')
IDENTITY = object()  # Ensure IDENTITY is unique.


def _dict_to_j_func(dict_mapping: Dict, default_value):
    java_map = dtypes.HashMap(dict_mapping)
    if default_value is IDENTITY:
        return _JPythonTools.functionFromMapWithIdentityDefaults(java_map)
    return _JPythonTools.functionFromMapWithDefault(java_map, default_value)


def _build_j_column_definition(col_name: str, data_type: DType, component_type=None):
    col = Column(name=col_name, data_type=data_type, component_type=component_type)
    return col.j_column_definition


def _build_j_column_definitions(ts):
    """ Convert a sequence of tuples of the form ('Price', double_type)
        or ('Prices', double_array_type, double_type) TODO vector type, check with Cristian
        to a list of ColumnDefinition objects.

    Args:
        ts: a sequence of 2 or 3 element tuples of (str, type) or (str, type, type)  specifying a column definition object.

    Returns:
        a list of column definition objects.
    """
    r = []
    for t in ts:
        r.append(_build_j_column_definition(*t))
    return r


def consume(
        kafka_config: Dict,
        topic: str,
        partitions: Any = None,
        offsets: Any = None,
        key: Any = None,
        value: Any = None,
        table_type: str = 'stream'
):
    """ Consume from Kafka to a Deephaven table.

    Args:
        kafka_config (Dict): dictionary with properties to configure the associated Kafka consumer and
            also the resulting table.  Once the table-specific properties are stripped, the result is
            passed to the org.apache.kafka.clients.consumer.KafkaConsumer constructor; pass any
            KafkaConsumer specific desired configuration here.
        topic (str): the topic name
        partitions : either a sequence of integer partition numbers or the predefined constant
            ALL_PARTITIONS for all partitions.
        offsets: either a dict mapping partition numbers to offset numbers, or one of the predefined constants
            ALL_PARTITIONS_SEEK_TO_BEGINNING, ALL_PARTITIONS_SEEK_TO_END or ALL_PARTITIONS_DONT_SEEK.
            If a dict, the values may be one of the predefined constants SEEK_TO_BEGINNING, SEEK_TO_END
            or DONT_SEEK.
        key: a specification for how to map the Key field in Kafka messages.  This should be the result of calling one
            of the methods simple, avro or json in this module, or None to obtain a single column specified in the
            kafka _config param via the keys 'deephaven.key.column.name' for column name and 'deephaven.key.column.type'
            for the column type; both should have string values associated to them.
        value: a specification for how to map the Value field in Kafka messages.  This should be the result of calling
            one of the methods simple, avro or json in this module, or None to obtain a single column specified in the
            kafka_config param via the keys 'deephaven.value.column.name' for column name and
            'deephaven.value.column.type' for the column type; both should have string values associated to them.
        table_type (str): a string specifying the resulting table type: one of 'stream' (default), 'append', 'stream_map'
            or 'append_map'.

    Returns:
        a Deephaven live table that will update based on Kafka messages consumed for the given topic.

    Raises:
        ValueError, TypeError, DHError
    """

    if not isinstance(topic, str):
        raise ValueError("argument 'topic' has to be of str type, instead got " + topic)

    if partitions is None:
        partitions = ALL_PARTITIONS
    elif isinstance(partitions, collections.Sequence):
        try:
            j_arr = jpy.array('int', partitions)
        except Exception as e:
            raise ValueError(
                "when not one of the predefined constants, keyword argument 'partitions' has to " +
                "represent a sequence of integer partition with values >= 0, instead got " +
                str(partitions) + " of type " + type(partitions).__name__
            ) from e
        partitions = _JKafkaTools.partitionFilterFromArray(j_arr)
    elif not isinstance(partitions, jpy.JType):
        raise TypeError(
            "argument 'partitions' has to be of str or sequence type, " +
            "or a predefined compatible constant, instead got partitions " +
            str(partitions) + " of type " + type(partitions).__name__)

    if offsets is None:
        offsets = ALL_PARTITIONS_DONT_SEEK
    elif isinstance(offsets, dict):
        try:
            partitions_array = jpy.array('int', list(offsets.keys()))
            offsets_array = jpy.array('long', list(offsets.values()))
            offsets = _JKafkaTools.partitionToOffsetFromParallelArrays(partitions_array, offsets_array)
        except Exception as e:
            raise ValueError(
                "when of type dict, keyword argument 'offsets' has to map " +
                "numeric partitions to either numeric offsets, or one of the constants { " +
                "SEEK_TO_BEGINNING, DONT_SEEK, SEEK_TO_END }," +
                "instead got offsets=" + str(offsets)
            ) from e
    elif not isinstance(offsets, jpy.JType):
        raise TypeError(
            "value " + str(offsets) + " of type " + type(offsets).__name__ +
            "  not recognized for argument 'offsets'; only str, dict like, or predefined constants allowed")

    if key is None:
        key = FROM_PROPERTIES
    if value is None:
        value = FROM_PROPERTIES
    if key is IGNORE and value is IGNORE:
        raise ValueError(
            "at least one argument for 'key' or 'value' must be different from IGNORE")

    if not isinstance(table_type, str):
        raise TypeError(
            "argument 'table_type' expected to be of type str, instead got " +
            str(table_type) + " of type " + type(table_type).__name__)
    table_type_enum = _JKafkaTools.friendlyNameToTableType(table_type)
    if table_type_enum is None:
        raise ValueError("unknown value " + table_type + " for argument 'table_type'")

    kafka_config = dtypes.Properties(kafka_config)
    try:
        return Table(
            j_table=_JKafkaTools.consumeToTable(kafka_config, topic, partitions, offsets, key, value, table_type_enum))
    except Exception as e:
        raise DHError(e, "failed to consume a Kafka stream.") from e


def avro(schema, schema_version: str = None, mapping: dict = None, mapping_only: dict = None):
    """ Specify an Avro schema to use when consuming a Kafka stream to a Deephaven table.

    Args:
        schema: either an Avro schema object or a string specifying a schema name for a schema
            registered in a Confluent compatible Schema Server.  When the latter is provided, the
            associated kafka_config dict in the call to consumeToTable should include the key
            'schema.registry.url' with the associated value of the Schema Server URL for fetching the schema
            definition.
        schema_version: if a string schema name is provided, the version to fetch from schema
            service; if not specified, a default of 'latest' is assumed.
        mapping: a dict representing a string to string mapping from Avro field name to Deephaven table
            column name; the fields mentioned in the mapping will have their column names defined by it; any other
            fields not mentioned in the mapping with use the same Avro field name for Deephaven table column
            name.  Note that only one parameter between mapping and mapping_only can be provided.
        mapping_only: a dict representing a string to string mapping from Avro field name to Deephaven
            table column name;  the fields mentioned in the mapping will have their column names defined by it;
            any other fields not mentioned in the mapping will be ignored and will not be present in the resulting
            table.  Note that only one parameter between mapping and mapping_only can be provided.

    Returns:
        a Kafka Key or Value spec object to use in a call to consumeToTable.

    Raises:
        ValueError, TypeError, DHError
    """
    if mapping is not None and mapping_only is not None:
        raise Exception(
            "only one argument between 'mapping' and " +
            "'mapping_only' expected, instead got both")
    if mapping is not None:
        have_mapping = True
        # when providing 'mapping', fields names not given are mapped as identity
        mapping = _dict_to_j_func(mapping, default_value=IDENTITY)
    elif mapping_only is not None:
        have_mapping = True
        # when providing 'mapping_only', fields not given are ignored.
        mapping = _dict_to_j_func(mapping_only, default_value=None)
    else:
        have_mapping = False
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

    try:
        if have_mapping:
            if have_actual_schema:
                return _JKafkaTools_Consume.avroSpec(schema, mapping)
            else:
                return _JKafkaTools_Consume.avroSpec(schema, schema_version, mapping)
        else:
            if have_actual_schema:
                return _JKafkaTools_Consume.avroSpec(schema)
            else:
                return _JKafkaTools_Consume.avroSpec(schema, schema_version)
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec") from e


def json(col_defs, mapping: dict = None):
    """ Specify how to use JSON data when consuming a Kafka stream to a Deephaven table.

    Args:
        col_defs:  a sequence of tuples specifying names and types for columns to be
            created on the resulting Deephaven table.  Tuples contain two elements, a
            string for column name and a Deephaven type for column data type.
        mapping:   a dict mapping JSON field names to column names defined in the col_defs
            argument.  If not present or None, a 1:1 mapping between JSON fields and Deephaven
            table column names is assumed.
    Returns:
        a Kafka Key or Value spec object to use in a call to consumeToTable.

    Raises:
        ValueError, TypeError, DHError
    """
    if not isinstance(col_defs, collections.abc.Sequence) or isinstance(col_defs, str):
        raise TypeError(
            "'col_defs' argument needs to be a sequence of tuples, instead got " +
            str(col_defs) + " of type " + type(col_defs).__name__)
    try:
        col_defs = _build_j_column_definitions(col_defs)
    except Exception as e:
        raise Exception("could not create column definitions from " + str(col_defs)) from e
    if mapping is None:
        return _JKafkaTools_Consume.jsonSpec(col_defs)
    if not isinstance(mapping, dict):
        raise TypeError(
            "argument 'mapping' is expected to be of dict type, " +
            "instead got " + str(mapping) + " of type " + type(mapping).__name__)
    mapping = dtypes.HashMap(mapping)
    try:
        return _JKafkaTools_Consume.jsonSpec(col_defs, mapping)
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec") from e


def simple(column_name: str, data_type: DType = None):
    """ Specify a single value when consuming a Kafka stream to a Deephaven table.

    Args:
        column_name:  a string specifying the Deephaven column name to use.
        data_type:  a Deephaven type specifying the column data type to use.

    Returns:
        a Kafka Key or Value spec object to use in a call to consumeToTable.

    Raises:
        TypeError, DHError
    """
    if not isinstance(column_name, str):
        raise TypeError(
            "'column_name' argument needs to be of str type, instead got " + str(column_name))
    try:
        if data_type is None:
            return _JKafkaTools_Consume.simpleSpec(column_name)
        return _JKafkaTools_Consume.simpleSpec(column_name, data_type.qst_type.clazz())
    except Exception as e:
        raise DHError(e, "failed to create a Kafka key/value spec") from e
