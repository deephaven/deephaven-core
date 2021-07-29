

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

def _custom_simpleConsumeToTable(kafkaConsumerPropertiesDict, topicName):
    return _java_type_.simpleConsumeToTable(dictToProperties(kafkaConsumerPropertiesDict), topicName)


# Define all of our functionality, if currently possible
try:
    _defineSymbols()
except Exception as e:
    pass
@_passThrough
def simpleConsumeToTable(kafkaConsumerProperties, topic):
    """
    :param kafkaConsumerProperties: java.util.Properties
    :param topic: java.lang.String
    :return: io.deephaven.db.tables.Table
    """
    
    return _custom_simpleConsumeToTable(kafkaConsumerProperties, topic)
