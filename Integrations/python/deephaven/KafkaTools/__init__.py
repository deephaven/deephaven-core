

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
from ..conversion_utils import _isJavaType, _isStr

_java_type_ = None  # None until the first _defineSymbols() call


def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _java_type_, _java_file_type_, _dh_config_, _compression_codec_
    if _java_type_ is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_ = jpy.get_type("io.deephaven.kafka.KafkaTools")


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
def _custom_simpleConsumeToTable(*args):
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

    return _java_type_.simpleConsumeToTable(
        dictToProperties(kafkaConsumerPropertiesDict),
        topicName,
        partitionFilter,
        partitionToInitialOffset)


# Define all of our functionality, if currently possible
try:
    _defineSymbols()
except Exception as e:
    pass

@_passThrough
def getAvroSchema(schemaServerUrl, group, schemaId, schemaVersion):
    """
    :param schemaServerUrl: java.lang.String
    :param group: java.lang.String
    :param schemaId: java.lang.String
    :param schemaVersion: java.lang.String
    :return: org.apache.avro.Schema
    """
    
    return _java_type_.getAvroSchema(schemaServerUrl, group, schemaId, schemaVersion)


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


@_passThrough
def simpleConsumeToTable(*args):
    """
    *Overload 1*  
      :param kafkaConsumerProperties: java.util.Properties
      :param topic: java.lang.String
      :param partitionFilter: java.util.function.IntPredicate
      :param partitionToInitialOffset: java.util.function.IntToLongFunction
      :return: io.deephaven.db.tables.Table
      
    *Overload 2*  
      :param kafkaConsumerProperties: java.util.Properties
      :param topic: java.lang.String
      :param partitionFilter: java.util.function.IntPredicate
      :return: io.deephaven.db.tables.Table
      
    *Overload 3*  
      :param kafkaConsumerProperties: java.util.Properties
      :param topic: java.lang.String
      :return: io.deephaven.db.tables.Table
    """
    
    return _custom_simpleConsumeToTable(*args)
