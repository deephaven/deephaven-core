

#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

##############################################################################
#               This code is auto generated. DO NOT EDIT FILE!
# Run generatePythonIntegrationStaticMethods or
# "./gradlew :Generators:generatePythonIntegrationStaticMethods" to generate
##############################################################################


import sys
import jpy
import wrapt
from ..conversion_utils import _isJavaType, _isStr, _tupleToColDef, _tuplesListToColDefsList

# None until the first _defineSymbols() call
_java_type_ = None
_stream_table_tools = None

def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _java_type_, _stream_table_tools_
    if _java_type_ is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_ = jpy.get_type("io.deephaven.kafka.KafkaTools")
        _stream_table_tools_ = jpy.get_type("io.deephaven.db.v2.StreamTableTools")


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
def dictToProperties(dict):
    JProps = jpy.get_type("java.util.Properties")
    r = JProps()
    for key, value in dict.items():
        r.setProperty(key, value)
    return r


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
def _commonKafkaArgs(args):
    if len(args) < 2:
        raise Exception('not enough arguments')
    kafkaConsumerPropertiesDict = args[0]
    topicName = args[1]
    if len(args) >= 3:
        partitionFilter = args[2]
        if isinstance(partitionFilter, str):
            partitionFilter = getattr(_java_type_, partitionFilter)
        else:
            partitionFilter = _java_type_.partitionFilterFromArray(jpy.array('int', partitionFilter))
    else:
        partitionFilter = getattr(_java_type_, 'ALL_PARTITIONS')

    if len(args) == 4:
        partitionToInitialOffset = args[3];
        if isinstance(partitionToInitialOffset, str):
            partitionToInitialOffset = getattr(_java_type_, partitionToInitialOffset)
        elif isinstance(partitionToInitialOffset, dict):
            dict = args[3];
            partitionsArray = jpy.array('int', dict.keys())
            offsetsArray = jpy.array('long', dict.values())
            partitionToInitialOffset = _java_type_.partitionToOffsetFromParallelArrays(partitionsArray, offsetsArray)
        else:
            raise Exception('wrong type for 4th argument partitionToInitialOffset: str or dict allowed')
    elif len(args) > 4:
        raise Exception('too many arguments')
    else:
        partitionToInitialOffset = getattr(_java_type_, 'ALL_PARTITIONS_DONT_SEEK')

    return [
        dictToProperties(kafkaConsumerPropertiesDict),
        topicName,
        partitionFilter,
        partitionToInitialOffset
    ]


@_passThrough
def consumeToTable(*args, **kwargs):
    r = _commonKafkaArgs(args)
    key_avro_schema = kwargs.pop('key_avro_schema', None)
    value_avro_schema = kwargs.pop('value_avro_schema', None)
    value_json = kwargs.pop('value_json', None)
    if (key_avro_schema == None and value_avro_schema == None and value_json == None):
        # simple key/value case.
        streaming_table = _java_type_.consumeToTable(r[0], r[1], r[2], r[3])
    elif (key_avro_schema != None or value_avro_schema != None):
        if (value_json != None):
            raise Exception('mixing json and avro not supported.')
        mapping = getattr(_java_type_, 'DIRECT_MAPPING')
        streaming_table = _java_type_.consumeToTable(r[0], r[1], r[2], r[3], key_avro_schema, mapping, value_avro_schema, mapping)
    elif (value_json != None):
        if not isinstance(value_json, list):
            raise Exception('value_json keyword argument should be a list.')
        col_def_list = _tuplesListToColDefsList(value_json)
        streaming_table = _java_type_.consumeJsonToTable(r[0], r[1], r[2], r[3], col_def_list)
    else:
        raise Exception('Unknown keyword arguments: ' + kwargs)
    table_type = kwargs.pop('table_type', None)
    if table_type == None or table_type == 'append':
        return _stream_table_tools_.streamToAppendOnlyTable(streaming_table)
    elif table_type == 'streaming':
        return streaming_table
    else:
        raise Exception('unknown table_type=' + table_type)

# Define all of our functionality, if currently possible
try:
    _defineSymbols()
except Exception as e:
    pass

@_passThrough
def avroSchemaToColumnDefinitions(*args):
    """
    *Overload 1*  
      :param mappedOut: java.util.Map<java.lang.String,java.lang.String>
      :param schema: org.apache.avro.Schema
      :param fieldNameMapping: java.util.function.Function<java.lang.String,java.lang.String>
      :return: io.deephaven.db.tables.ColumnDefinition<?>[]
      
    *Overload 2*  
      :param schema: org.apache.avro.Schema
      :param fieldNameMapping: java.util.function.Function<java.lang.String,java.lang.String>
      :return: io.deephaven.db.tables.ColumnDefinition<?>[]
      
    *Overload 3*  
      :param schema: org.apache.avro.Schema
      :return: io.deephaven.db.tables.ColumnDefinition<?>[]
    """
    
    return _java_type_.avroSchemaToColumnDefinitions(*args)


@_passThrough
def fieldNameMappingFromParallelArrays(fieldNames, columnNames):
    """
    :param fieldNames: java.lang.String[]
    :param columnNames: java.lang.String[]
    :return: java.util.function.Function<java.lang.String,java.lang.String>
    """
    
    return _java_type_.fieldNameMappingFromParallelArrays(fieldNames, columnNames)


@_passThrough
def getAvroSchema(schemaServerUrl, resourceName, version):
    """
    :param schemaServerUrl: java.lang.String
    :param resourceName: java.lang.String
    :param version: java.lang.String
    :return: org.apache.avro.Schema
    """
    
    return _java_type_.getAvroSchema(schemaServerUrl, resourceName, version)


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
