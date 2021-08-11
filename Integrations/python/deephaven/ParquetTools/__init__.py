
"""
Tools for managing and manipulating tables on disk in parquet format.
"""


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
_java_file_type_ = None
_dh_config_ = None
_compression_codec_ = None


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
        _java_type_ = jpy.get_type("io.deephaven.db.tables.utils.ParquetTools")
        _java_file_type_ = jpy.get_type("java.io.File")
        _dh_config_ = jpy.get_type("io.deephaven.configuration.Configuration")
        _compression_codec_ = jpy.get_type("org.apache.parquet.hadoop.metadata.CompressionCodecName")


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
def getFileObject(input):
    """
    Helper function for easily creating a java file object from a path string
    :param input: path string, or list of path strings
    :return: java File object, or java array of File objects
    """

    if _isJavaType(input):
        return input
    elif _isStr(input):
        return _java_file_type_(input)
    elif isinstance(input, list):
        # NB: map() returns an iterator in python 3, so list comprehension is appropriate here
        return jpy.array("java.io.File", [_java_file_type_(el) for el in input])
    else:
        raise ValueError("Method accepts only a java type, string, or list of strings as input. "
                         "Got {}".format(type(input)))


@_passThrough
def getWorkspaceRoot():
    """
    Helper function for extracting the root directory for the workspace configuration
    """
    return _dh_config_.getInstance().getWorkspacePath()

def _custom_deleteTable(path):
    return _java_type_.deleteTable(getFileObject(path))

def _custom_readTable(*args):
    if len(args) == 1:
        return _java_type_.readTable(getFileObject(args[0]))
    elif len(args) == 2:
        return _java_type_.readTable(getFileObject(args[0]), getattr(_java_type_, args[1]))
    else:
        return _java_type_.readTable(getFileObject(args[0]), getattr(_java_type_, args[1]), *args[2:])


def _custom_writeParquetTables(sources, tableDefinition, codecName, destinations, groupingColumns):
    if _isStr(codecName):
        return _java_type_.writeParquetTables(sources, tableDefinition, getattr(_compression_codec_, codecName),
                                              getFileObject(destinations), groupingColumns)
    else:
        return _java_type_.writeParquetTables(sources, tableDefinition, codecName,
                                              getFileObject(destinations), groupingColumns)


def _custom_writeTable(*args):
    if len(args) == 2:
        return _java_type_.writeTable(args[0], getFileObject(args[1]))
    elif len(args) == 3:
        return _java_type_.writeTable(args[0], getFileObject(args[1]),  getattr(_java_type_, args[2]))


def _custom_writeTables(sources, tableDefinition, destinations):
    return _java_type_.writeTables(sources, tableDefinition, getFileObject(destinations))


# Define all of our functionality, if currently possible
try:
    _defineSymbols()
except Exception as e:
    pass

@_passThrough
def convertSchema(schema, keyValueMetadata, readInstructionsIn):
    """
    Convert schema information from a ParquetMetadata into ColumnDefinitions.
    
    :param schema: (org.apache.parquet.schema.MessageType) - Parquet schema.
                               DO NOT RELY ON ParquetMetadataConverter FOR THIS! USE ParquetFileReader!
    :param keyValueMetadata: (java.util.Map<java.lang.String,java.lang.String>) - Parquet key-value metadata map
    :param readInstructionsIn: (io.deephaven.db.v2.parquet.ParquetInstructions) - Input conversion ParquetInstructions
    :return: (io.deephaven.base.Pair<java.util.List<io.deephaven.db.tables.ColumnDefinition>,io.deephaven.db.v2.parquet.ParquetInstructions>) A Pair with ColumnDefinitions and adjusted ParquetInstructions
    """
    
    return _java_type_.convertSchema(schema, keyValueMetadata, readInstructionsIn)


@_passThrough
def deleteTable(path):
    """
    Deletes a table on disk.
    
    :param path: (java.io.File) - path to delete
    """
    
    return _custom_deleteTable(path)


@_passThrough
def getParquetFileReader(parquetFile):
    """
    Make a ParquetFileReader for the supplied File.
    
    :param parquetFile: (java.io.File) - The File to read
    :return: (io.deephaven.parquet.ParquetFileReader) The new ParquetFileReader
    """
    
    return _java_type_.getParquetFileReader(parquetFile)


@_passThrough
def readParquetSchemaAndTable(source, readInstructionsIn, instructionsOut):
    """
    :param source: java.io.File
    :param readInstructionsIn: io.deephaven.db.v2.parquet.ParquetInstructions
    :param instructionsOut: org.apache.commons.lang3.mutable.MutableObject<io.deephaven.db.v2.parquet.ParquetInstructions>
    :return: io.deephaven.db.tables.Table
    """
    
    return _java_type_.readParquetSchemaAndTable(source, readInstructionsIn, instructionsOut)


@_passThrough
def readPartitionedTable(locationKeyFinder, readInstructions, tableDefinition):
    """
    Reads in a table from files discovered with locationKeyFinder using the provided table definition.
    
    :param locationKeyFinder: (io.deephaven.db.v2.locations.impl.TableLocationKeyFinder<io.deephaven.db.v2.locations.parquet.local.ParquetTableLocationKey>) - The source of location keys to include
    :param readInstructions: (io.deephaven.db.v2.parquet.ParquetInstructions) - Instructions for customizations while reading
    :param tableDefinition: (io.deephaven.db.tables.TableDefinition) - The table's definition
    :return: (io.deephaven.db.tables.Table) The table
    """
    
    return _java_type_.readPartitionedTable(locationKeyFinder, readInstructions, tableDefinition)


@_passThrough
def readPartitionedTableInferSchema(locationKeyFinder, readInstructions):
    """
    Reads in a table from files discovered with locationKeyFinder using a definition built from the
     first location found, which must have non-null partition values for all partition keys.
    
    :param locationKeyFinder: (io.deephaven.db.v2.locations.impl.TableLocationKeyFinder<io.deephaven.db.v2.locations.parquet.local.ParquetTableLocationKey>) - The source of location keys to include
    :param readInstructions: (io.deephaven.db.v2.parquet.ParquetInstructions) - Instructions for customizations while reading
    :return: (io.deephaven.db.tables.Table) The table
    """
    
    return _java_type_.readPartitionedTableInferSchema(locationKeyFinder, readInstructions)


@_passThrough
def readPartitionedTableWithMetadata(directory, readInstructions):
    """
    Reads in a table using metadata files found in the supplied directory.
    
    :param directory: (java.io.File) - The source of location keys to include
    :param readInstructions: (io.deephaven.db.v2.parquet.ParquetInstructions) - Instructions for customizations while reading
    :return: (io.deephaven.db.tables.Table) The table
    """
    
    return _java_type_.readPartitionedTableWithMetadata(directory, readInstructions)


@_passThrough
def readSingleFileTable(tableLocationKey, readInstructions, tableDefinition):
    """
    Reads in a table from a single parquet file using the provided table definition.
    
    :param tableLocationKey: (io.deephaven.db.v2.locations.parquet.local.ParquetTableLocationKey) - The location keys to include
    :param readInstructions: (io.deephaven.db.v2.parquet.ParquetInstructions) - Instructions for customizations while reading
    :param tableDefinition: (io.deephaven.db.tables.TableDefinition) - The table's definition
    :return: (io.deephaven.db.tables.Table) The table
    """
    
    return _java_type_.readSingleFileTable(tableLocationKey, readInstructions, tableDefinition)


@_passThrough
def readTable(*args):
    """
    Reads in a table from a single parquet, metadata file, or directory with recognized layout.
    
    *Overload 1*  
      :param sourceFilePath: (java.lang.String) - The file or directory to examine
      :return: (io.deephaven.db.tables.Table) table
      
    *Overload 2*  
      :param sourceFilePath: (java.lang.String) - The file or directory to examine
      :param readInstructions: (io.deephaven.db.v2.parquet.ParquetInstructions) - Instructions for customizations while reading
      :return: (io.deephaven.db.tables.Table) table
      
    *Overload 3*  
      :param sourceFile: (java.io.File) - The file or directory to examine
      :return: (io.deephaven.db.tables.Table) table
      
    *Overload 4*  
      :param sourceFile: (java.io.File) - The file or directory to examine
      :param readInstructions: (io.deephaven.db.v2.parquet.ParquetInstructions) - Instructions for customizations while reading
      :return: (io.deephaven.db.tables.Table) table
    """
    
    return _custom_readTable(*args)


@_passThrough
def setDefaultCompressionCodecName(compressionCodecName):
    """
    :param compressionCodecName: java.lang.String
    """
    
    return _java_type_.setDefaultCompressionCodecName(compressionCodecName)


@_passThrough
def writeParquetTables(sources, tableDefinition, writeInstructions, destinations, groupingColumns):
    """
    Writes tables to disk in parquet format to a supplied set of destinations.  If you specify grouping columns, there
     must already be grouping information for those columns in the sources.  This can be accomplished with
     .by(<grouping columns>).ungroup() or .sort(<grouping column>).
    
    :param sources: (io.deephaven.db.tables.Table[]) - The tables to write
    :param tableDefinition: (io.deephaven.db.tables.TableDefinition) - The common schema for all the tables to write
    :param writeInstructions: (io.deephaven.db.v2.parquet.ParquetInstructions) - Write instructions for customizations while writing
    :param destinations: (java.io.File[]) - The destinations paths.    Any non existing directories in the paths provided are created.
                              If there is an error any intermediate directories previously created are removed;
                              note this makes this method unsafe for concurrent use
    :param groupingColumns: (java.lang.String[]) - List of columns the tables are grouped by (the write operation will store the grouping info)
    """
    
    return _custom_writeParquetTables(sources, tableDefinition, writeInstructions, destinations, groupingColumns)


@_passThrough
def writeTable(*args):
    """
    Write a table to a file.
    
    *Overload 1*  
      :param sourceTable: (io.deephaven.db.tables.Table) - source table
      :param destPath: (java.lang.String) - destination file path; the file name should end in ".parquet" extension
                          If the path includes non-existing directories they are created
                          If there is an error any intermediate directories previously created are removed;
                          note this makes this method unsafe for concurrent use
      
    *Overload 2*  
      :param sourceTable: (io.deephaven.db.tables.Table) - source table
      :param destFile: (java.io.File) - destination file; the file name should end in ".parquet" extension
                          If the path includes non-existing directories they are created
      
    *Overload 3*  
      :param sourceTable: (io.deephaven.db.tables.Table) - source table
      :param destFile: (java.io.File) - destination file; its path must end in ".parquet".  Any non existing directories in the path are created
                          If there is an error any intermediate directories previously created are removed;
                          note this makes this method unsafe for concurrent use
      :param definition: (io.deephaven.db.tables.TableDefinition) - table definition to use (instead of the one implied by the table itself)
      
    *Overload 4*  
      :param sourceTable: (io.deephaven.db.tables.Table) - source table
      :param destFile: (java.io.File) - destination file; its path must end in ".parquet".  Any non existing directories in the path are created
                                If there is an error any intermediate directories previously created are removed;
                                note this makes this method unsafe for concurrent use
      :param writeInstructions: (io.deephaven.db.v2.parquet.ParquetInstructions) - instructions for customizations while writing
      
    *Overload 5*  
      :param sourceTable: (io.deephaven.db.tables.Table) - source table
      :param destPath: (java.lang.String) - destination path; it must end in ".parquet".  Any non existing directories in the path are created
                                If there is an error any intermediate directories previously created are removed;
                                note this makes this method unsafe for concurrent use
      :param definition: (io.deephaven.db.tables.TableDefinition) - table definition to use (instead of the one implied by the table itself)
      :param writeInstructions: (io.deephaven.db.v2.parquet.ParquetInstructions) - instructions for customizations while writing
      
    *Overload 6*  
      :param sourceTable: (io.deephaven.db.tables.Table) - source table
      :param destFile: (java.io.File) - destination file; its path must end in ".parquet".  Any non existing directories in the path are created
                                If there is an error any intermediate directories previously created are removed;
                                note this makes this method unsafe for concurrent use
      :param definition: (io.deephaven.db.tables.TableDefinition) - table definition to use (instead of the one implied by the table itself)
      :param writeInstructions: (io.deephaven.db.v2.parquet.ParquetInstructions) - instructions for customizations while writing
    """
    
    return _custom_writeTable(*args)


@_passThrough
def writeTables(sources, tableDefinition, destinations):
    """
    Write out tables to disk.
    
    :param sources: (io.deephaven.db.tables.Table[]) - source tables
    :param tableDefinition: (io.deephaven.db.tables.TableDefinition) - table definition
    :param destinations: (java.io.File[]) - destinations
    """
    
    return _custom_writeTables(sources, tableDefinition, destinations)
