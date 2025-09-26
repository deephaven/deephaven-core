---
title: Parquet instructions
---

All of the specific, detailed instructions for reading Parquet files into Deephaven tables are passed into `readTable` as an instance of the [`ParquetInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.html) class. This class is used to specify the layout of the Parquet files, the table definition, and any special instructions for reading the files.

## ParquetInstructions

A `ParquetInstructions` instance is created using the `ParquetInstructions.builder()` method, which returns a [`ParquetInstructions.Builder`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html) instance. Instructions are specified by calling the builder's methods, and then the `build()` method to create the `ParquetInstructions` instance. For example, to specify the layout of the Parquet files as key-value partitioned, use the following code:

```groovy order=taxi
import io.deephaven.parquet.table.ParquetInstructions
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout

// create ParquetInstructions instance with key-value partitioned layout
instructionsInstance = ParquetInstructions.builder().setFileLayout(ParquetFileLayout.valueOf("SINGLE_FILE")).build()

// pass instructionsInstance to readTable
taxi = ParquetTools.readTable("/data/examples/Taxi/parquet/taxi.parquet", instructionsInstance)
```

### `ParquetInstructions` methods

The `ParquetInstructions` class has the following methods:

- `baseNameForPartitionedParquetData()`: Returns the base name for partitioned parquet data. Can be set with `Builder.setBaseNameForPartitionedParquetData`.
- `builder()`: Returns a new `ParquetInstructions.Builder` instance.
- `generateMetadataFiles()`: Returns a boolean indicating whether the `ParquetInstructions` instance is set to generate "\_metadata" and "\_common_metadata" files while writing parquet files.
- `getCodecArgs(columnName)`: Returns the codec arguments for the specified column.
- `getCodecName(columnName)`: Returns the codec name for the specified column.
- `getColumnNameFromParquetColumnName(parquetColumnName)`: Returns the column name in the Deephaven table corresponding to the specified Parquet column name.
- `getColumnNameFromParquetColumnNameOrDefault(parquetColumnName)`: Returns the column name in the Deephaven table corresponding to the specified Parquet column name, or the Parquet column name if no mapping exists.
- `getCompressionCodecName()`: Returns the compression codec name.
- `getDefaultCompressionCodecName()`: Returns the default compression codec name.
- `getDefaultMaximumDictionaryKeys()`: Returns the default maximum dictionary keys.
- `getDefaultMaximumDictionarySize()`: Returns the default maximum dictionary size.
- `getDefaultTargetPageSize()`: Returns the default target page size.
- `getFileLayout()`: Returns the Parquet file layout.
- `getIndexColumns()`: Returns the index columns.
- `getMaximumDictionaryKeys()`: Returns the maximum dictionary keys.
- `getMaximumDictionarySize()`: Returns the maximum dictionary size.
- `getParquetColumnNameFromColumnNameOrDefault(columnName)`: Returns the Parquet column name corresponding to the specified column name, or the column name if no mapping exists.
- `getRowGroupInfo()`: Returns the [`RowGroupInfo`](/core/javadoc/io/deephaven/parquet/table/metadata/RowGroupInfo.html) for this `ParquetInstructions` instance.
- `getSpecialInstructions()`: Returns the special instructions set for this `ParquetInstructions` instance.
- `getTableDefinition()`: Returns the table definition.
- `getTargetPageSize()`: Returns the target page size.
- `isLegacyParquet()`: Returns a boolean indicating whether the Parquet data is in legacy format.
- `isRefreshing()`: Returns a boolean indicating whether the Parquet data represents a refreshing source.
- `sameColumnNamesAndCodecMappings(i1, i2)`: Returns a boolean indicating whether the two `ParquetInstructions` instances have the same column names and codec mappings.
- `setDefaultMaximumDictionaryKeys(maximumDictionaryKeys)`: Sets the default maximum dictionary keys.
- `setDefaultMaximumDictionarySize(maximumDictionarySize)`: Sets the default maximum dictionary size.
- `setDefaultTargetPageSize(newDefaultSizeBytes)`: Sets the default target page size.
- `useDictionary(columnName)`: Returns a boolean indicating whether the specified column uses dictionary encoding.
- `withLayout(fileLayout)`: Returns a new `ParquetInstructions` instance with the supplied `ParquetFileLayout`.
- `withTableDefinition(tableDefinition)`: Returns a new `ParquetInstructions` instance with the supplied table definition.
- `withTableDefinitionAndLayout(tableDefinition, fileLayout)`: Returns a new `ParquetInstructions` instance with the supplied table definition and `ParquetFileLayout`.

### `ParquetInstructions.Builder` methods

The `ParquetInstructions.Builder` class has the following methods:

- `addAllIndexColumns(indexColumns)`: Adds provided lists of columns to persist together as indexes.
  This method accepts an Iterable of lists, where each list represents a group of columns to be indexed together. The write operation will store the index info as sidecar tables. This argument is used to narrow the set of indexes to write, or to be explicit about the expected set of indexes present on all sources. Indexes that are specified but missing will be computed on demand. To prevent the generation of index files, provide an empty iterable.
- `addColumnCodec(columnName, codecName)`: Adds a column codec mapping between the provided column name and codec name.
- `addColumnNameMapping(parquetColumnName, columnName)`: Adds a column name mapping between the provided Parquet column name and Deephaven column name.
- `addIndexColumns(indexColumns...)`: Add a list of columns to persist together as indexes. The write operation will store the index info as sidecar tables. This argument is used to narrow the set of indexes to write, or to be explicit about the expected set of indexes present on all sources. Indexes that are specified but missing will be computed on demand.
- `build()`: Builds the `ParquetInstructions` instance.
- `getTakenNames()`: Returns a set of column names that have been taken.
- `setBaseNameForPartitionedParquetData(baseNameForPartitionedParquetData)`: Sets the base name for partitioned parquet data.
- `setCompressionCodecName(compressionCodecName)`: The name of the [compression codec](https://www.javadoc.io/doc/org.apache.parquet/parquet-hadoop/1.8.1/org/apache/parquet/hadoop/metadata/CompressionCodecName.html) to use. This defines the particular type of compression used for the given column and can have significant implications for the speed of the import. The options are:
  - `SNAPPY`: (default) Aims for high speed and a reasonable amount of compression. Based on [Google](https://github.com/google/snappy/blob/main/format_description.txt)'s Snappy compression format.
  - `UNCOMPRESSED`: The output will not be compressed.
  - `LZ4_RAW`: A codec based on the [LZ4 block format](https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md). Should always be used instead of `LZ4`.
  - `LZO`: Compression codec based on or interoperable with the [LZO compression library](https://www.oberhumer.com/opensource/lzo/).
  - `GZIP`: Compression codec based on the GZIP format (not the closely-related "zlib" or "deflate" formats) defined by [RFC 1952](https://tools.ietf.org/html/rfc1952).
  - `ZSTD`: Compression codec with the highest compression ratio based on the Zstandard format defined by [RFC 8478](https://tools.ietf.org/html/rfc8478).
  - `LZ4`: **Deprecated** Compression codec loosely based on the [LZ4 compression algorithm](https://github.com/lz4/lz4), but with an additional undocumented framing scheme. The framing is part of the original Hadoop compression library and was historically copied first in parquet-mr, then emulated with mixed results by parquet-cpp. Note that `LZ4` is deprecated; use `LZ4_RAW` instead.
- `setFileLayout(fileLayout)`: Sets the Parquet file layout. Use with `ParquetFileLayout.valueOf(<Enum>)`. If this method is not called, layout is inferred. Enums are:
  - "SINGLE_FILE": A single Parquet file.
  - "FLAT_PARTITIONED": A single directory of Parquet files with no nested subdirectories.
  - "KV_PARTITIONED": A directory of Parquet files partitioned by key-value pairs.
  - "METADATA_PARTITIONED": This layout can be used to describe either a single Parquet "\_metadata" or "\_common_metadata" file, or a directory containing a "\_metadata" file and an optional "\_common_metadata" file.
- `setGenerateMetadataFiles(generateMetadataFiles)`: Sets whether or not to generate "\_metadata" and "\_common_metadata" files while writing Parquet files.
- `setIsLegacyParquet(isLegacyParquet)`: Sets whether the Parquet data is in legacy format.
- `setIsRefreshing(isRefreshing)`: Sets whether the Parquet data represents a refreshing source.
- `setMaximumDictionaryKeys(maximumDictionaryKeys)`: Sets the maximum dictionary keys.
- `setMaximumDictionarySize(maximumDictionarySize)`: Set the maximum number of bytes the writer should add to the dictionary before switching to non-dictionary encoding; never evaluated for non-String columns, ignored if use dictionary is set for the column.
- `setRowGroupInfo(rowGroupInfo)`: Sets the Row Group type used for writing. Available Row Group types are:
  - [`RowGroupInfo.singleGroup()`](/core/javadoc/io/deephaven/parquet/table/metadata/RowGroupInfo.html#singleGroup()): The default `RowGroupInfo` implementation. All data is written within a single Row Group.
  - [`RowGroupInfo.maxRows(maxRows)`](/core/javadoc/io/deephaven/parquet/table/metadata/RowGroupInfo.html#maxRows(long)): Splits into a number of Row Groups, each of which has no more than the requested number of rows.
  - [`RowGroupInfo.maxGroups(numRowGroups)`](/core/javadoc/io/deephaven/parquet/table/metadata/RowGroupInfo.html#maxGroups(long)): Splits evenly into a pre-defined number of Row Groups, each of which contains the same number of rows. If the input table size is not evenly divisible but the number of Row Groups requested, then some Row Groups will contain one fewer row.
  - [`RowGroupInfo.byGroups(groups)`](/core/javadoc/io/deephaven/parquet/table/metadata/RowGroupInfo.html#byGroups(java.lang.String...)): Splits each unique group into a Row Group. If the input table does not have all values for the group(s) contiguously, then an exception will be thrown during the `writeTable(...)` call.
  - [`RowGroupInfo.byGroups(maxRows, groups)`](/core/javadoc/io/deephaven/parquet/table/metadata/RowGroupInfo.html#byGroups(long,java.lang.String...)): Splits each unique group into a Row Group. If the input table does not have all values for the group(s) contiguously, then an exception will be thrown during the `writeTable(...)` call. If a given Row Group yields a row count greater than `maxRows`, then it will be split further using the same logic as `RowGroupInfo.maxRows(maxRows)`.
- `setSpecialInstructions(specialInstructions)`: Special instructions for reading Parquet files, useful when reading files from a non-local S3 server. These instructions are provided as an instance of [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions).
- `setTableDefinition(tableDefinition)`: Sets the table definition.
- `setTargetPageSize(targetPageSize)`: Sets the target page size.
- `useDictionary(columnName, useDictionary)`: Set a hint that the writer should use dictionary-based encoding for writing this column; never evaluated for non-String columns.

### `S3Instructions` methods

The `S3Instructions` class has the following methods:

- `append(LogOutput)`:
- `builder()`: Returns a new `S3Instructions.Builder` instance.
- `connectionTimeout()`: A Duration representing the amount of time to wait for a successful S3 connection before timing out. The default is 2 seconds.
- `credentials()`: The `Credentials` to use for reading files. Options are:
  - `Credentials.anonymous()`: Use anonymous credentials.
  - `Credentials.basic(accessKeyId, secretAccessKey)`: Use basic credentials with the specified access key ID and secret access key.
  - `Credentials.defaultCredentials()`: Use the default credentials.
- `endpointOverride()`: The endpoint to connect to. Callers connecting to AWS do not typically need to set this; it is most useful when connecting to non-AWS, S3-compatible APIs. The default is `None`
- `fragmentSize()`: The maximum byte size of each fragment to read from S3. Defaults to 65536; must be larger than 8192.
- `maxConcurrentRequests()`: The maximum number of concurrent requests to make to S3. Defaults to 256.
- `numConcurrentWriterParts()`: The maximum number of parts that can be uploaded concurrently when writing to S3 without blocking.
- `readAheadCount()`: The number of fragments asynchronously read ahead of the current fragment as the current fragment is being read. The default is `1`.
- `readTimeout()`: The amount of time it takes to time out while reading a fragment. The default is 2 seconds.
- `regionName()`: The region name of the AWS S3 bucket where the Parquet data exists. If this is not set, it is picked by the AWS SDK from 'aws.region' system property, "AWS_REGION" environment variable, the `{user.home}/.aws/credentials` or `{user.home}/.aws/config` files, or from EC2 metadata service, if running in EC2. If no region name is derived from the above chain or the region name derived is incorrect for the bucket accessed, the correct region name will be derived internally, at the cost of one additional request.
- `writePartSize()`: The size of each part (in bytes) to upload when writing to S3. Default is 10485760.

## Related documentation

- [Supported Parquet formats](../../conceptual/parquet-formats.md)
- [Import Parquet files](./parquet-import.md)
- [Export Parquet files](./parquet-export.md)
