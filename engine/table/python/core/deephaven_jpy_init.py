#!/usr/bin/python

# DO NOT DELETE THESE IMPORTS:
# dill and base64 are required for our PickledResult to function properly
import dill
import base64

import jpy
import sys
import os
import pandas
import logging

# Set stdin to /dev/null to prevent functions (like help()) that attempt to read from stdin from hanging the worker.
os.dup2(os.open("/dev/null", os.O_RDONLY), 0)

jpy.VerboseExceptions.enabled = True
# If you want jpy to tell you about all that it is doing, change this
# jpy.diag.flags = jpy.diag.F_ALL

from deephaven import Config
import deephaven.TableTools as ttools
from deephaven.python_to_java import dataFrameToTable

# NOTE: **THIS IS REQUIRED** for WorkerPythonEnvironment - don't take it out...
import __main__


###############################################################################################

# can't use IsWidget.get_dh_table or the data frame will be interpreted as a table instead of a widget?
pandas.DataFrame.to_dh_table = lambda self: dataFrameToTable(self, True)


########################################################################################################
# Performance monitoring

def add_tables_from_map_to_binding(tables):
    """
    Iterates through a (java) map of tables and adds them to the global binding. This is a helper method
    accommodating for the fact that the performance queries return the table results as a (java) HashMap object.

    :param tables: java HashMap
    """

    it = tables.entrySet().iterator()
    while it.hasNext():
        pair = it.next()
        globals()[pair.getKey()] = pair.getValue()
        it.remove()


def query_update_performance_set(evaluationNumber):
    """
    Name matched to Groovy equivalent for coherent usage.
    TODO: document this

    :param worker_name:
    :param date:
    :param use_intraday:
    :param server_host:
    :param as_of_time:
    :return:
    """

    _jtype_ = jpy.get_type("io.deephaven.engine.table.impl.util.PerformanceQueries")

    tableMap = _jtype_.queryUpdatePerformanceMap(evaluationNumber)
    add_tables_from_map_to_binding(tableMap)

# Convenience functions for a variety of purposes


def importjava(clazz):
    _jclass_ = jpy.get_type("java.lang.Class")
    ql = jpy.get_type("io.deephaven.engine.table.lang.QueryLibrary")

    def _get_short_class_name_(clazz):
        return _jclass_.forName(clazz).getSimpleName()

    clazz = clazz.strip()
    javaclass = _get_short_class_name_(clazz)
    globals()[javaclass] = jpy.get_type(clazz)
    ql.importClass(_jclass_.forName(clazz))


def importstatic(clazz):
    _jclass_ = jpy.get_type("java.lang.Class")
    ql = jpy.get_type("io.deephaven.engine.table.lang.QueryLibrary")
    clazz = clazz.strip()
    javaclass = jpy.get_type(clazz)
    for key, value in javaclass.__dict__.items():
        if not key.startswith("__"):
            if hasattr(value, 'methods'):
                methods = value.methods
                if methods:
                    if methods[0].is_static:
                        globals()[value.name] = value

    ql.importClass(_jclass_.forName(clazz))


def java_array(type, values):
    return jpy.array(type, values)


def IntArray(values):
    return java_array('int', values)


def DoubleArray(values):
    return java_array('double', values)


def FloatArray(values):
    return java_array('float', values)


def LongArray(values):
    return java_array('long', values)


def ShortArray(values):
    return java_array('short', values)


def BooleanArray(values):
    return java_array('boolean', values)


def ByteArray(values):
    return java_array('byte', values)


def _exists_and_is_file_(prop):
    """
    For finding a fully qualified customer configured import file

    :param prop: a stem of the import file
    :return: the fully qualified file path
    """

    config = Config()
    file_path = config.getStringWithDefault(prop, None)
    if file_path:
        file_path = file_path.replace("<devroot>", config.getDevRootPath())
        if os.path.isfile(file_path):
            return file_path

    return None


def _empty_or_comment_(line):
    """helper method for extracting a line"""
    return line is None or len(line.strip()) < 1 or line.strip().startswith("#")


######################################################################################################
# Perform the desired imports for the console workspace
# NOTE: these can't be moved into a method or the imports would be out of scope for the console

default_imports = _exists_and_is_file_("python.default.imports")
if default_imports:
    # test if line matches pattern "import <stuff>" or "from <stuff> import <stuff>"
    import re

    import_re = '^(?:import|(from(\s+)(.+))import)(\s+)(.+)'
    import_pattern = re.compile(import_re)

    with open(default_imports, 'r') as f:
        for line in f:
            # note that this pattern is repeated, but I want to avoid a method polluting the namespace
            if _empty_or_comment_(line):
                # this line is empty or a comment
                continue

            if ";" in line:
                logging.error("Can not run line \n{}\n from python.default.imports, contains a ';'".format(line))
                continue

            try:
                if import_pattern.match(line):
                    exec(line)
                else:
                    logging.error("Could not run line \n{}\n from python.default.imports, "
                                  "does not match import pattern" .format(line))
            except ImportError:
                logging.error("Could not import module: {}".format(line))
    del import_re, import_pattern

default_imports = _exists_and_is_file_("python.default.javaclass.imports")
if default_imports:
    with open(default_imports) as f:
        for line in f:
            if _empty_or_comment_():
                continue

            # noinspection PyBroadException
            try:
                importjava(line)
            except Exception:
                logging.error("Could not import java class: {}".format(line))

# clean up the workspace, since this is forwarded as the console namespace
del _empty_or_comment_, _exists_and_is_file_, default_imports
