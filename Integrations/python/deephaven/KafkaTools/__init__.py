

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
        _avro_schema_type_ = jpy.get_type("org.apache.avro.Schema")
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

    if not isinstance(mapping, dict):
        raise Exception("argument 'mapping' has to be of dict type, instead got " +
                        str(mapping) + " of type " + type(mapping).__name__)
    field_names = jpy.array('java.lang.String', mapping.keys())
    column_names = jpy.array('java.lang.String', mapping.values())
    mapping = _java_type_.fieldNameMappingFromParallelArrays(field_names, column_names)
    return _java_type_.avroSchemaToColumnDefinitions(schema, mapping)


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
    if not isinstance(kafka_config, dict):
        raise Exception("argument 'kafka_config' has to be of type dict, instead got " + str(kafka_config))

    if not _isStr(topic):
        raise Exception("argument 'topic' has to be of str type, instead got " + topic)

    if partitions is None:
        partitions = ALL_PARTITIONS
    elif isinstance(partitions, collections.Sequence):
        try:
            jarr = jpy.array('int', partitions)
        except Exception as e:
            raise Exception(
                "when not one of the predefined constants, keyword argument 'partitions' has to " +
                "represent a sequence of integer partition values >= 0"
            ) from e
        partitions = _java_type_.partitionFilterFromArray(jarr)
    elif not isinstance(partitions, jpy.JType):
        raise Exception("argument 'partitions' has to be of str or sequence type, " +
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
            raise Exception(
                "when of type dict, keyword argument 'offsets' has to map " +
                "numeric partitions to either numeric offsets, or one of the constants { " +
                "SEEK_TO_BEGINNING, DONT_SEEK, SEEK_TO_END }," +
                "instead got offsets=" + str(offsets)
            ) from e
    elif not isinstance(offsets, jpy.JType):
        raise Exception(
            "value " + str(offsets) + " of type " + type(offsets).__name__ +
            "  not recognized for argument 'offsets'; only str, dict or predefined constants allowed")

    if key is None:
        key = FROM_PROPERTIES
    if value is None:
        value = FROM_PROPERTIES
    if key is IGNORE and value is IGNORE:
        raise Exception(
            "at least one argument for 'key' or 'value' must be different from the default IGNORE")

    if not _isStr(table_type):
        raise Exception("argument 'table_type' expected to be of type str, instead got " +
                        str(table_type) + " of type " + type(table_type).__name__)
    table_type_enum = _java_type_.friendlyNameToTableType(table_type)
    if table_type_enum is None:
        raise Exception("unknown value " + table_type + " for argument 'table_type'")

    kafka_config = _dictToProperties(kafka_config)
    return _java_type_.consumeToTable(kafka_config, topic, partitions, offsets, key, value, table_type_enum)


@_passThrough
def avro(schema, schema_version:str = None, mapping:dict = None, mapping_only:dict = None):
    if mapping is not None and mapping_only is not None:
        raise Exception(
            "only one argument between 'mapping' and " +
            "'mapping_only' expected, instead got both")
    if mapping is not None:
        have_mapping = True
        if not instanceof(mapping, dict):
            raise Exception("'mapping' argument is expected to be of dict type, " +
                            "instead got " + str(mapping) + " of type " + type(mapping).__name__)
        # when providing 'mapping_only', fields names not given are mapped as identity
        mapping = _dictToFun(mapping, default_value=IDENTITY)
    elif mapping_only is not None:
        have_mapping = True
        if not instanceof(mapping, dict):
            raise Exception("'mapping_only' argument is expected to be of dict type, " +
                            "instead found " + str(mapping_only) + " of type " + type(mapping_only).__name__)
        # when providing 'mapping_only', fields not given are ignored.
        mapping = _dictToFun(mapping, default_value=None)
    else:
        have_mapping = False
    if _isStr(schema):
        have_actual_schema = False
        if schema_version is None:
            schema_version = "latest"
        elif not _isStr(schema_version):
            raise Exception("argument 'schema_version' should be of str type, instead got " +
                            str(schema_version) + " of type " + type(schema_version).__name__)
    elif instanceof(schema, _avro_schema_jtype_):
        have_actual_schema = True
        if schema_version is not None:
            raise Exception("argument 'schema_version' is only expected if schema is of str type")
    else:
        raise Exception("'schema' argument expected to be of either " +
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
    if not isinstance(col_defs, collections.Sequence) or _isStr(col_defs):
        raise Exception("'col_defs' argument needs to be a sequence of tuples, instead got " +
                        str(col_defs) + " of type " + type(col_defs).__name__)
    try:
        col_defs = dh._colDefs(col_defs)
    except Exception as e:
        raise Exception("could not create column definitions from " + str(col_defs)) from e
    if mapping is None:
        return _java_type_.jsonSpec(col_defs)
    if not isinstance(mapping, dict):
        raise Exception(
            "argument 'mapping' is expected to be of dict type, " +
            "instead got " + str(mapping) + " of type " + type(mapping).__name__)
    mapping = _dictToMap(mapping)
    return _java_type_.jsonSpec(col_defs, mapping)


@_passThrough
def simple(column_name:str, data_type:dh.DataType = None):
    if not _isStr(column_name):
        raise Exception("'column_name' argument needs to be of str type, instead got " + str(column_name))
    if data_type is None:
        return _java_type_.simpleSpec(column_name)
    return _java_type_.simpleSpec(column_name, _jclassFromType(data_type))


@_passThrough
def streamTableToAppendTable(t):
    return _stream_table_tools_.streamToAppendOnlyTable(t)
    
# Define all of our functionality, if currently possible
try:
    _defineSymbols()
except Exception as e:
    pass
@_passThrough
def avroSchemaToColumnDefinitions(*args):
    """
    *Overload 1*  
      :param columns: java.util.List<io.deephaven.db.tables.ColumnDefinition>
      :param mappedOut: java.util.Map<java.lang.String,java.lang.String>
      :param schema: org.apache.avro.Schema
      :param fieldNameToColumnName: java.util.function.Function<java.lang.String,java.lang.String>
      
    *Overload 2*  
      :param columns: java.util.List<io.deephaven.db.tables.ColumnDefinition>
      :param schema: org.apache.avro.Schema
      :param fieldNameToColumnName: java.util.function.Function<java.lang.String,java.lang.String>
      
    *Overload 3*  
      :param columns: java.util.List<io.deephaven.db.tables.ColumnDefinition>
      :param schema: org.apache.avro.Schema
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
    *Overload 1*  
      :param schemaServerUrl: java.lang.String
      :param resourceName: java.lang.String
      :param version: java.lang.String
      :return: org.apache.avro.Schema
      
    *Overload 2*  
      :param schemaServerUrl: java.lang.String
      :param resourceName: java.lang.String
      :return: org.apache.avro.Schema
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
