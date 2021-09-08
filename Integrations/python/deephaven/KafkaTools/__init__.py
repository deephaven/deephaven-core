

# -*-Python-*-
#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

##############################################################################
#               This code is auto generated. DO NOT EDIT FILE!
# Run generatePythonIntegrationStaticMethods or
# "./gradlew :Generators:generatePythonIntegrationStaticMethods" to generate
##############################################################################


import collections
import sys
import jpy
import wrapt

import deephaven.Types as dh

from ..conversion_utils import _isJavaType, _isStr, \
    _typeFromName, _dictToProperties, _dictToMap, IDENTITY

from ..Types import _jclassFromType

# None until the first _defineSymbols() call
_java_type_ = None
_stream_table_tools_ = None
_avro_schema_jtype_ = None
SEEK_TO_BEGINNING = None
DONT_SEEK = None
SEEK_TO_END = None
FROM_PROPERTIES = None
IGNORE = None
ALL_PARTITIONS = None
ALL_PARTITIONS_SEEK_TO_BEGINNING = None
ALL_PARTITIONS_DONT_SEEK = None
ALL_PARTITIONS_SEEK_TO_END = None

def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _java_type_, _stream_table_tools_, _avro_schema_jtype_, \
        SEEK_TO_BEGINNING, DONT_SEEK, SEEK_TO_END, FROM_PROPERTIES, IGNORE, \
        ALL_PARTITIONS, ALL_PARTITIONS_SEEK_TO_BEGINNING, ALL_PARTITIONS_DONT_SEEK, ALL_PARTITIONS_SEEK_TO_END
    if _java_type_ is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_ = jpy.get_type("io.deephaven.kafka.KafkaTools")
        _stream_table_tools_ = jpy.get_type("io.deephaven.db.v2.StreamTableTools")
        _avro_schema_jtype_ = jpy.get_type("org.apache.avro.Schema")
        SEEK_TO_BEGINNING = getattr(_java_type_, 'SEEK_TO_BEGINNING')
        DONT_SEEK = getattr(_java_type_, 'DONT_SEEK')
        SEEK_TO_END = getattr(_java_type_, 'SEEK_TO_END')
        FROM_PROPERTIES = getattr(_java_type_, 'FROM_PROPERTIES')
        IGNORE = getattr(_java_type_, 'IGNORE')
        ALL_PARTITIONS = getattr(_java_type_, 'ALL_PARTITIONS')
        ALL_PARTITIONS_SEEK_TO_BEGINNING = getattr(_java_type_, 'ALL_PARTITIONS_SEEK_TO_BEGINNING')
        ALL_PARTITIONS_DONT_SEEK = getattr(_java_type_, 'ALL_PARTITIONS_DONT_SEEK')
        ALL_PARTITIONS_SEEK_TO_END = getattr(_java_type_, 'ALL_PARTITIONS_SEEK_TO_END')


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
def _custom_avroSchemaToColumnDefinitions(schema, mapping:dict = None):
    if mapping is None:
        return _java_type_.avroSchemaToColumnDefinitions(schema)

    field_names = jpy.array('java.lang.String', mapping.keys())
    column_names = jpy.array('java.lang.String', mapping.values())
    mapping = _java_type_.fieldNameMappingFromParallelArrays(field_names, column_names)
    return _java_type_.avroSchemaToColumnDefinitions(schema, mapping)


# Define all of our functionality, if currently possible
try:
    _defineSymbols()
except Exception as e:
    pass

@_passThrough
def consumeToTable(
        kafka_config:dict,
        topic:str,
        partitions = None,
        offsets = None,
        key = None,
        value = None,
        table_type = 'stream'
):
    """
    Consume from Kafka to a Deephaven table.

    :param kafka_config: Dictionary with properties to configure the associated kafka consumer and
        also the resulting table.  Once the table-specific properties are stripped, the result is
        passed to the org.apache.kafka.clients.consumer.KafkaConsumer constructor; pass any
        KafkaConsumer specific desired configuration here.
    :param topic: The topic name
    :param partitions: Either a sequence of integer partition numbers or the predefined constant
       ALL_PARTITIONS for all partitions.
    :param offsets: Either a dict mapping partition numbers to offset numbers, or one of the predefined constants
       ALL_PARTITIONS_SEEK_TO_BEGINNING, ALL_PARTITIONS_SEEK_TO_END or ALL_PARTITIONS_DONT_SEEK.
       If a dict, the values may be one of the predefined constants SEEK_TO_BEGINNING, SEEK_TO_END
       or DONT_SEEK.
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

    if not _isStr(topic):
        raise ValueError("argument 'topic' has to be of str type, instead got " + topic)

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

    if offsets is None:
        offsets = ALL_PARTITIONS_DONT_SEEK
    elif isinstance(offsets, dict):
        try:
            partitions_array = jpy.array('int', offsets.keys())
            offsets_array = jpy.array('long', offsets.values())
            offsets = _java_type_.partitionToOffsetFromParallelArrays(partitions_array, offsets_array)
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

    if not _isStr(table_type):
        raise TypeError(
            "argument 'table_type' expected to be of type str, instead got " +
            str(table_type) + " of type " + type(table_type).__name__)
    table_type_enum = _java_type_.friendlyNameToTableType(table_type)
    if table_type_enum is None:
        raise ValueError("unknown value " + table_type + " for argument 'table_type'")

    kafka_config = _dictToProperties(kafka_config)
    return _java_type_.consumeToTable(kafka_config, topic, partitions, offsets, key, value, table_type_enum)


@_passThrough
def avro(schema, schema_version:str = None, mapping:dict = None, mapping_only:dict = None):
    """
    Specify an Avro schema to use when consuming a Kafka stream to a Deephaven table.

    :param schema:  Either an Avro schema object or a string specifying a schema name for a schema
       registered in a Confluent compatible Schema Server.  When the latter is provided, the
       associated kafka_config dict in the call to consumeToTable should include the key
       'schema.registry.url' with the associated value of the Schema Server URL for fetching the schema
       definition.
    :param schema_version:   If a string schema name is provided, the version to fetch from schema
       service; if not specified, a default of 'latest' is assumed.
    :param mapping: A dict representing a string to string mapping from Avro field name to Deephaven table
       column name; the fields mentioned in the mapping will have their column names defined by it; any other
       fields not mentioned in the mapping with use the same Avro field name for Deephaven table column
       name.  Note that only one parameter between mapping and mapping_only can be provided.
    :param mapping_only: A dict representing a string to string mapping from Avro field name to Deephaven
       table column name;  the fields mentioned in the mapping will have their column names defined by it;
       any other fields not mentioned in the mapping will be ignored and will not be present in the resulting
       table.  Note that only one parameter between mapping and mapping_only can be provided.
    :return:  A Kafka Key or Value spec object to use in a call to consumeToTable.
    :raises:  ValueError, TypeError or Exception if arguments provided can't be processed.
    """
    if mapping is not None and mapping_only is not None:
        raise Exception(
            "only one argument between 'mapping' and " +
            "'mapping_only' expected, instead got both")
    if mapping is not None:
        have_mapping = True
        # when providing 'mapping_only', fields names not given are mapped as identity
        mapping = _dictToFun(mapping, default_value=IDENTITY)
    elif mapping_only is not None:
        have_mapping = True
        # when providing 'mapping_only', fields not given are ignored.
        mapping = _dictToFun(mapping, default_value=None)
    else:
        have_mapping = False
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

    if have_mapping:
        if have_actual_schema:
            return _java_type_.avroSpec(schema, mapping)
        else:
            return _java_type_.avroSpec(schema, schema_version, mapping)
    else:
        if have_actual_schema:
            return _java_type_.avroSpec(schema)
        else:
            return _java_type_.avroSpec(schema, schema_version)
    

@_passThrough
def json(col_defs, mapping:dict = None):
    """
    Specify how to use JSON data when consuming a Kafka stream to a Deephaven table.

    :param col_defs:  A sequence of tuples specifying names and types for columns to be
       created on the resulting Deephaven table.  Tuples contain two elements, a
       string for column name and a Deephaven type for column data type.
    :param mapping:   A dict mapping JSON field names to column names defined in the col_defs
       argument.  If not present or None, a 1:1 mapping between JSON fields and Deephaven
       table column names is assumed.
    :return:  A Kafka Key or Value spec object to use in a call to consumeToTable.
    :raises:  ValueError, TypeError or Exception if arguments provided can't be processed.
    """
    if not isinstance(col_defs, collections.Sequence) or _isStr(col_defs):
        raise TypeError(
            "'col_defs' argument needs to be a sequence of tuples, instead got " +
            str(col_defs) + " of type " + type(col_defs).__name__)
    try:
        col_defs = dh._colDefs(col_defs)
    except Exception as e:
        raise Exception("could not create column definitions from " + str(col_defs)) from e
    if mapping is None:
        return _java_type_.jsonSpec(col_defs)
    if not isinstance(mapping, dict):
        raise TypeError(
            "argument 'mapping' is expected to be of dict type, " +
            "instead got " + str(mapping) + " of type " + type(mapping).__name__)
    mapping = _dictToMap(mapping)
    return _java_type_.jsonSpec(col_defs, mapping)


@_passThrough
def simple(column_name:str, data_type:dh.DataType = None):
    """
    Specify a single value when consuming a Kafka stream to a Deephaven table.

    :param column_name:  A string specifying the Deephaven column name to use.
    :param data_type:  A Deephaven type specifying the column data type to use.
    :return:  A Kafka Key or Value spec object to use in a call to consumeToTable.
    :raises:  TypeError if arguments provided can't be processed.
    """
    if not _isStr(column_name):
        raise TypeError(
            "'column_name' argument needs to be of str type, instead got " + str(column_name))
    if data_type is None:
        return _java_type_.simpleSpec(column_name)
    return _java_type_.simpleSpec(column_name, _jclassFromType(data_type))


@_passThrough
def streamTableToAppendTable(t):
    """
    Creates a 'stream' table from an 'append' type.

    :param t:  The 'stream' table input.
    :return:  The resulting 'append' table.
    """
    return _stream_table_tools_.streamToAppendOnlyTable(t)
@_passThrough
def avroSchemaToColumnDefinitions(*args):
    """
    **Incompatible overloads text - text from the first overload:**
    
    Convert an Avro schema to a list of column definitions, mapping every avro field to a column of the same name.
    
    *Overload 1*  
      :param columns: java.util.List<io.deephaven.db.tables.ColumnDefinition<?>>
      :param mappedOut: java.util.Map<java.lang.String,java.lang.String>
      :param schema: org.apache.avro.Schema
      :param fieldNameToColumnName: java.util.function.Function<java.lang.String,java.lang.String>
      
    *Overload 2*  
      :param columns: (java.util.List<io.deephaven.db.tables.ColumnDefinition<?>>) - Column definitions for output; should be empty on entry.
      :param schema: (org.apache.avro.Schema) - Avro schema
      :param fieldNameToColumnName: (java.util.function.Function<java.lang.String,java.lang.String>) - An optional mapping to specify selection and naming of columns from Avro fields, or
              null for map all fields using field name for column name.
      
    *Overload 3*  
      :param columns: (java.util.List<io.deephaven.db.tables.ColumnDefinition<?>>) - Column definitions for output; should be empty on entry.
      :param schema: (org.apache.avro.Schema) - Avro schema
    """
    
    return _java_type_.avroSchemaToColumnDefinitions(*args)


@_passThrough
def friendlyNameToTableType(typeName):
    """
    Map "Python-friendly" table type name to a KafkaTools.TableType.
    
    :param typeName: (java.lang.String) - The friendly name
    :return: (io.deephaven.kafka.KafkaTools.TableType) The mapped KafkaTools.TableType
    """
    
    return _java_type_.friendlyNameToTableType(typeName)


@_passThrough
def getAvroSchema(*args):
    """
    **Incompatible overloads text - text from the first overload:**
    
    Fetch an Avro schema from a Confluent compatible Schema Server.
    
    *Overload 1*  
      :param schemaServerUrl: (java.lang.String) - The schema server URL
      :param resourceName: (java.lang.String) - The resource name that the schema is known as in the schema server
      :param version: (java.lang.String) - The version to fetch, or the string "latest" for the latest version.
      :return: (org.apache.avro.Schema) An Avro schema.
      
    *Overload 2*  
      :param schemaServerUrl: (java.lang.String) - The schema server URL
      :param resourceName: (java.lang.String) - The resource name that the schema is known as in the schema server
      :return: (org.apache.avro.Schema) An Avro schema.
    """
    
    return _java_type_.getAvroSchema(*args)


@_passThrough
def partitionFilterFromArray(partitions):
    """
    :param partitions: int[]
    :return: java.util.function.IntPredicate
    """
    
    return _java_type_.partitionFilterFromArray(partitions)


@_passThrough
def partitionToOffsetFromParallelArrays(partitions, offsets):
    """
    :param partitions: int[]
    :param offsets: long[]
    :return: java.util.function.IntToLongFunction
    """
    
    return _java_type_.partitionToOffsetFromParallelArrays(partitions, offsets)
