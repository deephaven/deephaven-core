

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

from ..conversion_utils import _isJavaType, _isStr, _tupleToColDef, _tuplesListToColDefsList, _typeFromName, _dictToProperties, _dictToMap

# None until the first _defineSymbols() call
_java_type_ = None
_stream_table_tools_ = None
_avro_schema_jtype_ = None
SEEK_TO_BEGINNING = None
DONT_SEEK = None
FROM_PROPERTIES = None
IGNORE = None
ALL_PARTITIONS = None
ALL_PARTITIONS_SEEK_TO_BEGINNING = None
ALL_PARTITIONS_DONT_SEEK = None

def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _java_type_, _stream_table_tools_, _avro_schema_jtype_, SEEK_TO_BEGINNING, DONT_SEEK, FROM_PROPERTIES, IGNORE
    global ALL_PARTITIONS, ALL_PARTITIONS_DONT_SEEK, ALL_PARTITIONS_SEEK_TO_BEGINNING
    if _java_type_ is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_ = jpy.get_type("io.deephaven.kafka.KafkaTools")
        _stream_table_tools_ = jpy.get_type("io.deephaven.db.v2.StreamTableTools")
        _avro_schema_type_ = jpy.get_type("org.apache.avro.Schema")
        SEEK_TO_BEGINNING = getattr(_java_type_, 'SEEK_TO_BEGINNING')
        DONT_SEEK = getattr(_java_type_, 'DONT_SEEK')
        FROM_PROPERTIES = getattr(_java_type_, 'FROM_PROPERTIES')
        IGNORE = getattr(_java_type_, 'IGNORE')
        ALL_PARTITIONS = getattr(_java_type_, 'ALL_PARTITIONS')
        ALL_PARTITIONS_SEEK_TO_BEGINNING = getattr(_java_type_, 'ALL_PARTITIONS_SEEK_TO_BEGINNING')
        ALL_PARTITIONS_DONT_SEEK = getattr(_java_type_, 'ALL_PARTITIONS_DONT_SEEK')


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
def _custom_avroSchemaToColumnDefinitions(*args):
    if len(args) == 0:
        raise Exception('not enough arguments')
    if len(args) == 1:
        return _java_type_.avroSchemaToColumnDefinitions(args[0])
    if len(args) == 2:
        schema = args[0]
        dict = args[1];
        fieldNamesArray = jpy.array('java.lang.String', dict.keys())
        columnNamesArray = jpy.array('java.lang.String', dict.values())
        mapping = _java_type_.fieldNameMappingFromParallelArrays(fieldNamesArray, columnNamesArray)
        return _java_type_.avroSchemaToColumnDefinitions(schema, mapping)
    raise Exception('too many arguments: ' + len(args))


@_passThrough
def consumeToTable(*args, **kwargs):
    if len(args) != 2:
        raise Exception('not enough positional arguments: expected 2, consumer properties and topic')
    if not isinstance(args[0], dict):
        raise Exception('argument 0 of type dict expected for kafka consumer properties')
    consumer_props = _dictToProperties(args[0])

    if not _isStr(args[1]):
        raise Exception('argument 1 of type str expected for topic name')
    topic = args[1]

    partitions = kwargs.pop('partitions', None)
    if partitions is None:
        partitions = ALL_PARTITIONS
    elif isinstance(partitions, collections.Sequence):
        try:
            jarr = jpy.array('int', partitionFilter)
        except Exception as e:
            raise Exception(
                "when not one of the predefined constants, keyword argument 'partitions' has to " +
                "represent a sequence of integer partition values >= 0"
            ) from e
        partitions = _java_type_.partitionFilterFromArray(jarr)
    elif not isinstance(partitions, jpy.JType):
        raise Exception("keyword argument 'partitions' has to be of type str or sequence, instead got partitions=" + str(partitions))

    offsets = kwargs.pop('offsets', None)
    if offsets is None:
        offsets = ALL_PARTITIONS_DONT_SEEK
    elif isinstance(offsets, dict):
        try:
            partitionsArray = jpy.array('int', offsets.keys())
            offsetsArray = jpy.array('long', offsets.values())
            partitionToInitialOffset = _java_type_.partitionToOffsetFromParallelArrays(partitionsArray, offsetsArray)
        except Exception as e:
            raise Exception(
                "when of type dict, keyword argument 'offsets' has to map " +
                "numeric partitions to either numeric offsets, or the constants DONT_SEEK and SEEK_TO_BEGINNING, " +
                "instead got offsets=" + str(offsets)
            ) from e
    elif not isinstance(offsets, jpy.JType):
        raise Exception(
            "type " + type(offsets).__name__ +
            "  of keyword argument 'offsets' not recognized; only str or dict allowed")

    key = kwargs.pop('key', FROM_PROPERTIES)
    value = kwargs.pop('value', FROM_PROPERTIES)
    if key is IGNORE and value is IGNORE:
        raise Exception(
            "at least one keyword argument for specifying either a key or value is required; " + 
            "they can't be both omitted, and they can't be both the IGNORE constant")

    table_type = kwargs.pop('table_type', None)
    if table_type is not None:
        if not _isStr(table_type):
            raise Exception("keyword argument 'table_type' expected to be of type str, instead got " +
                            str(table_type))
        if table_type != 'append' and table_type != 'streaming':
            raise Exception("unknown value " + table_type + " for keyword argument 'table_type'")

    if len(kwargs) > 0:
        raise Exception("excess keyword arguments not understood given: " + str(kwargs))

    streaming_table = _java_type_.consumeToTable(consumer_props, topic, partitions, offsets, key, value)
    if table_type is None or table_type == 'append':
        return _stream_table_tools_.streamToAppendOnlyTable(streaming_table)
    # table_type == 'streaming'
    return streaming_table


@_passThrough
def avro(schema, **kwargs):
    schema_version = kwargs.pop('schema_version', "latest")  # default may never be used if schema is not of str type.
    mapping = kwargs.pop('mapping', None)
    mapping_only = kwargs.pop('mapping_only', None)
    if mapping is not None and mapping_only is not None:
        raise Exception(
            "only one keyword argument between 'mapping' and " +
            "'mapping_only' expected, instead got both")
    if len(kwargs) > 0:
        raise Exception("excess keyword arguments not understood given: " + str(kwargs))
    if mapping is not None:
        have_mapping = True
        if not instanceof(mapping, dict):
            raise Exception("mapping keyword argument is expected to be of dict type, " +
                            "instead found " + str(dict_arg))
        # when providing 'mapping_only', fields names not given are mapped as identity
        mapping = _dictToFun(dict_arg)
    elif mapping_only is not None:
        have_mapping = True
        if not instanceof(mapping, dict):
            raise Exception("mapping_only keyword argument is expected to be of dict type, " +
                            "instead found " + str(dict_arg))
        # when providing 'mapping_only', fields not given are ignored.
        mapping = _dictToFun(dict_arg, default_value=None)
    else:
        have_mapping = False
    if _isStr(schema):
        have_actual_schema = False
    elif instanceof(schema, _avro_schema_jtype_):
        have_actual_schema = True
    else:
        raise Exception("first positional argument for schema expected to be of " +
                        "type str or avro schema type, instead got " + str(schema))
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
def json(col_defs, **kwargs):
    if not isinstance(col_defs, collections.Sequence) or _isStr(col_defs):
        raise Exception("first argument for column definitions needs to be a sequence, instead got " + str(col_defs))
    try:
        col_defs = _tuplesListToColDefsList(col_defs)
    except Exception as e:
        raise Exception("could not create column definitions from " + str(col_defs)) from e
    mapping = kwargs.pop('mapping', None)
    if len(kwargs) > 0:
        raise Exception("excess keyword arguments not understood given: " + str(kwargs))    
    if mapping is None:
        return _java_type_.jsonSpec(col_defs)
    if not isinstance(mapping, dict):
        raise Exception(
            "keyword argument 'mapping' is expected to be of type dict, " +
            "instead got " + str(mapping))
    mapping = _dictToMap(mapping)
    return _java_type_.jsonSpec(col_defs, mapping)


@_passThrough
def simple(*args):
    if len(args) < 1 or len(args) > 2:
        raise Exception("one or two arguments expected, instead got " + len(args))
    colName = args[0]
    if not _isStr(colName):
        raise Exception("column_name argument needs to be of str type, instead got " + colName)
    if len(args) == 1:
        return _java_type_.simpleSpec(colName)
    jTypeStr = args[1]
    if not _isStr(jTypeStr):
        raise Exception("type_name argument needs to be of str type, instead got " + jTypeStr)
    try:
        jType = _typeFromName(jTypeStr)
    except Exception as e:
        raise Exception("could not convert type name " + jTypeStr + " to type") from e
    return _java_type_.simpleSpec(colName, jType)


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
