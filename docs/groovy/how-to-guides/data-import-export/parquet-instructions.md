---
title: Parquet instructions
---

[`readTable`](../../reference/data-import-export/Parquet/readTable.md), [`writeTable`](../../reference/data-import-export/Parquet/writeTable.md), and [`writeKeyValuePartitionedTable`](../../reference/data-import-export/Parquet/writeKeyValuePartitionedTable.md) take their options as an instance of the [`ParquetInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.html) class. Use this class to specify the layout of the Parquet files, the table definition, and how the files are compressed and encoded. To read or write Parquet files on S3, pass an [`S3Instructions`](#s3instructions) instance with the connection settings to the builder's `setSpecialInstructions` method.

## `ParquetInstructions`

To create a [`ParquetInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.html) instance, call `ParquetInstructions.builder`, which returns a [`ParquetInstructions.Builder`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html) instance. Call the builder's methods to set options, and then call `build` to create the `ParquetInstructions` instance. For example, to specify that the source is a single Parquet file, use the following code:

```groovy test-set=1 order=taxi
import io.deephaven.parquet.table.ParquetInstructions
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout

// create ParquetInstructions instance with single-file layout
instructionsInstance = ParquetInstructions.builder().setFileLayout(ParquetFileLayout.SINGLE_FILE).build()

// pass instructionsInstance to readTable
taxi = ParquetTools.readTable("/data/examples/Taxi/parquet/taxi.parquet", instructionsInstance)
```

You build instructions for writing the same way. A [row group](./parquet-formats.md) is a block of rows within a Parquet file. The following example writes the `taxi` table with Zstandard compression and splits the file into row groups of at most 10,000 rows:

```groovy test-set=1 order=taxiZstd
import io.deephaven.parquet.table.metadata.RowGroupInfo

// create ParquetInstructions instance with write options
writeInstructions = ParquetInstructions.builder()
    .setCompressionCodecName("ZSTD")
    .setRowGroupInfo(RowGroupInfo.maxRows(10000))
    .build()

// pass writeInstructions to writeTable, then read the file back
ParquetTools.writeTable(taxi, "/data/taxi-zstd.parquet", writeInstructions)
taxiZstd = ParquetTools.readTable("/data/taxi-zstd.parquet")
```

### `ParquetInstructions.Builder` methods

Some builder methods apply only when reading, some only when writing, and some to both.

#### Reading options

- `setFileLayout(fileLayout)`: Sets the Parquet [file layout](./parquet-formats.md#file-layouts). If you don't call this method, Deephaven infers the layout. The available [`ParquetFileLayout`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html) values are:
  - `ParquetFileLayout.SINGLE_FILE`: A single Parquet file.
  - `ParquetFileLayout.FLAT_PARTITIONED`: A single directory of Parquet files with no nested subdirectories.
  - `ParquetFileLayout.KV_PARTITIONED`: A directory of Parquet files partitioned by key-value pairs.
  - `ParquetFileLayout.METADATA_PARTITIONED`: A single Parquet `_metadata` or `_common_metadata` file, or a directory containing a `_metadata` file and an optional `_common_metadata` file.
- `setIsLegacyParquet(isLegacyParquet)`: Sets whether to read a binary column as `String` instead of `byte[]` when the column has no Parquet logical type and the file's Deephaven metadata records no codec for it. The default is `false`.
- `setIsRefreshing(isRefreshing)`: Sets whether the resulting table refreshes to pick up new files added to the source directory. A read with the `SINGLE_FILE` or `METADATA_PARTITIONED` layout can't be refreshing.
- `setUnsignedLongTarget(columnName, target)`: Sets the Deephaven type to read the specified unsigned 64-bit integer (`UINT_64`) column as.
  - This setting applies only to columns with the `UINT_64` logical type.
  - Writes ignore it, because Deephaven never writes `UINT_64`.
  - You can't set two different targets for one column name.
  - A table definition supplied with `setTableDefinition` (see [Reading and writing options](#reading-and-writing-options)) governs the column type instead. If that definition disagrees with this target, `build` rejects it.
  - The available [`UnsignedLongTarget`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.UnsignedLongTarget.html) targets are:
    - `UnsignedLongTarget.BIG_INTEGER`: (default) Read the column as `java.math.BigInteger`, which represents every `UINT_64` value exactly.
    - `UnsignedLongTarget.LONG`: Read the column as `long`. Values greater than 2<sup>63</sup> - 1 have no `long` representation, so reading a page that contains one raises an error.
    - `UnsignedLongTarget.SIGNED_LONG`: Read the column as `long`, reinterpreting the bit pattern as signed. Values greater than 2<sup>63</sup> - 1 read as negative numbers, and 2<sup>63</sup> reads as `NULL_LONG`, which is indistinguishable from a null.

#### Writing options

- `addIndexColumns(indexColumns...)` and `addAllIndexColumns(indexColumns)`: Add columns to persist together as [indexes](../data-indexes.md#persist-data-indexes-with-parquet). `addIndexColumns` adds one list of columns as one index. `addAllIndexColumns` takes an iterable of lists, and each list is one index.
  - The write operation stores each index as a separate Parquet file in a `.dh_metadata/indexes/` subdirectory of the directory that holds the data file.
  - If you call neither method, the writer persists every index that already exists on the source table.
  - Use these methods to narrow the set of indexes to write, or to state the indexes expected on all sources.
  - Indexes that are specified but missing are computed on demand.
  - You can't add an index on a single partitioning column.
  - To prevent the generation of index files, pass an empty iterable to `addAllIndexColumns`.
- `setBaseNameForPartitionedParquetData(baseNameForPartitionedParquetData)`: Sets the base name of the files written for partitioned Parquet data. The default is `{uuid}`. The base name can contain these tokens:
  - `{i}`: An automatically incremented integer.
  - `{uuid}`: A random UUID.
  - `{partitions}`: The partition values as underscore-delimited `key=value` pairs. For example, `{partitions}-table` with partitioning columns `PC1` and `PC2` produces a file named like `PC1=partition1_PC2=partitionA-table.parquet`.
- `setCompressionCodecName(compressionCodecName)`: Sets the name of the [compression codec](https://www.javadoc.io/doc/org.apache.parquet/parquet-common/1.18.1/org/apache/parquet/hadoop/metadata/CompressionCodecName.html) used when writing Parquet files. The codec applies to every column in the file and affects write speed and file size. The options are:
  - `SNAPPY`: (default) Aims for high speed and a reasonable amount of compression. Based on Google's [Snappy compression format](https://github.com/google/snappy/blob/main/format_description.txt).
  - `UNCOMPRESSED`: The output is not compressed.
  - `LZ4_RAW`: A codec based on the [LZ4 block format](https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md).
  - `LZO`: Compression codec based on or interoperable with the [LZO compression library](https://www.oberhumer.com/opensource/lzo/).
  - `GZIP`: Compression codec based on the GZIP format defined by [RFC 1952](https://tools.ietf.org/html/rfc1952). This is a different format from the closely related zlib and deflate formats.
  - `ZSTD`: Compression codec with a high compression ratio based on the Zstandard format defined by [RFC 8478](https://tools.ietf.org/html/rfc8478).
  - `BROTLI`: Compression codec based on [Brotli](https://github.com/google/brotli), offering high compression ratios. Deephaven doesn't include a Brotli codec. To use this option, add a Brotli codec (`org.apache.hadoop.io.compress.BrotliCodec`) to the server classpath. Without one, the write fails with a `Failed to find CompressionCodec` error.
  - `LZ4`: **Deprecated.** Use `LZ4_RAW` instead.
- `setFieldId(columnName, fieldId)`: Sets the Parquet field ID written for the specified column. Most users don't need to set field IDs.
- `setGenerateMetadataFiles(generateMetadataFiles)`: Sets whether to generate `_metadata` and `_common_metadata` files while writing Parquet files.
- `setMaximumDictionaryKeys(maximumDictionaryKeys)`: Sets the maximum number of unique keys the writer adds to a column's dictionary before it switches to non-dictionary encoding. The writer applies this setting only to `String` columns and columns of `String` arrays or vectors. It ignores the setting for a column where `useDictionary` is set to `true`. The default is 1048576, the value of `ParquetInstructions.DEFAULT_MAXIMUM_DICTIONARY_KEYS`.
- `setMaximumDictionarySize(maximumDictionarySize)`: Sets the maximum number of bytes the writer adds to a column's dictionary before it switches to non-dictionary encoding. The writer applies this setting only to `String` columns and columns of `String` arrays or vectors. It ignores the setting for a column where `useDictionary` is set to `true`. The default is 1048576, the value of `ParquetInstructions.DEFAULT_MAXIMUM_DICTIONARY_SIZE`.
- `setOnWriteCompleted(onWriteCompleted)`: Sets a callback invoked after each Parquet data file is written, excluding index and metadata files.
- `setRowGroupInfo(rowGroupInfo)`: Sets how the writer splits the table into [row groups](./parquet-formats.md). The available options are:
  - [`RowGroupInfo.singleGroup`](/core/javadoc/io/deephaven/parquet/table/metadata/RowGroupInfo.html#singleGroup()): The default `RowGroupInfo` implementation. All data is written within a single row group.
  - [`RowGroupInfo.maxRows(maxRows)`](/core/javadoc/io/deephaven/parquet/table/metadata/RowGroupInfo.html#maxRows(long)): Splits the table into row groups of no more than `maxRows` rows each.
  - [`RowGroupInfo.maxGroups(numRowGroups)`](/core/javadoc/io/deephaven/parquet/table/metadata/RowGroupInfo.html#maxGroups(long)): Splits the table into the requested number of row groups of nearly equal size. If the row count isn't evenly divisible by the number of row groups, some row groups contain one fewer row than others.
  - [`RowGroupInfo.byGroups(groups)`](/core/javadoc/io/deephaven/parquet/table/metadata/RowGroupInfo.html#byGroups(java.lang.String...)): Splits each unique group into a row group. If the values of the grouping columns aren't contiguous in the input table, the [`writeTable`](../../reference/data-import-export/Parquet/writeTable.md) call throws an exception.
  - [`RowGroupInfo.byGroups(maxRows, groups)`](/core/javadoc/io/deephaven/parquet/table/metadata/RowGroupInfo.html#byGroups(long,java.lang.String...)): Splits each unique group into a row group. If the values of the grouping columns aren't contiguous in the input table, the `writeTable` call throws an exception. The writer splits any group with more than `maxRows` rows further, as `RowGroupInfo.maxRows(maxRows)` does.
- `setTargetPageSize(targetPageSize)`: Sets the target size, in bytes, of each data page the writer produces. The default is 65536, the value of `ParquetInstructions.DEFAULT_TARGET_PAGE_SIZE`. You can override it with the `Parquet.defaultTargetPageSize` [configuration property](../configuration/configuration-properties.md). The value must be at least the `Parquet.minTargetPageSize` configuration property, which defaults to 2048.
- `useDictionary(columnName, useDictionary)`: Sets a hint that the writer should keep dictionary encoding for this column. The writer already tries dictionary encoding for every `String` column and every column of `String` arrays or vectors. Setting the hint to `true` removes the `setMaximumDictionaryKeys` and `setMaximumDictionarySize` limits for the column, so the writer never falls back to non-dictionary encoding. The writer ignores this hint for other column types.

#### Reading and writing options

- `addColumnCodec(columnName, codecName)`: Maps a column to a column codec, the class Deephaven uses to convert that column's values to and from bytes. `codecName` is the codec's class name.
- `addColumnCodec(columnName, codecName, codecArgs)`: Adds a column codec mapping between the provided column name and codec name, with arguments for the codec.
- `addColumnNameMapping(parquetColumnName, columnName)`: Adds a column name mapping between the provided Parquet column name and Deephaven column name.
- `setSpecialInstructions(specialInstructions)`: Sets special instructions for reading or writing Parquet files at an S3 URI (`s3://`, `s3a://`, or `s3n://`, including S3-compatible stores) or a Google Cloud Storage URI (`gs://`). Pass an [`S3Instructions`](#s3instructions) instance.
- `setTableDefinition(tableDefinition)`: Sets the table definition to use instead of the one inferred from the Parquet files when reading, or from the source table when writing.

#### Other builder methods

- `build`: Builds the `ParquetInstructions` instance.
- `getTakenNames`: Returns the set of Deephaven column names that already have per-column settings on this builder. The per-column settings come from `addColumnNameMapping`, `addColumnCodec`, `setFieldId`, `setUnsignedLongTarget`, and `useDictionary`.

The builder also has methods for integrations such as Iceberg, including `setColumnResolverFactory`. Users don't typically call these methods, so this page doesn't list them.

### `ParquetInstructions` methods

A `ParquetInstructions` instance has an accessor for each builder setting listed above:

| Builder method                          | `ParquetInstructions` accessor                                                                                           |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `addAllIndexColumns`, `addIndexColumns` | `getIndexColumns`                                                                                                        |
| `addColumnCodec`                        | `getCodecName(columnName)`, `getCodecArgs(columnName)`                                                                   |
| `addColumnNameMapping`                  | `getColumnNameFromParquetColumnName(parquetColumnName)`                                                                  |
| `setBaseNameForPartitionedParquetData`  | `baseNameForPartitionedParquetData`                                                                                      |
| `setCompressionCodecName`               | `getCompressionCodecName`                                                                                                |
| `setFieldId`                            | `getFieldId(columnName)`                                                                                                 |
| `setFileLayout`                         | `getFileLayout`                                                                                                          |
| `setGenerateMetadataFiles`              | `generateMetadataFiles`                                                                                                  |
| `setIsLegacyParquet`                    | `isLegacyParquet`                                                                                                        |
| `setIsRefreshing`                       | `isRefreshing`                                                                                                           |
| `setMaximumDictionaryKeys`              | `getMaximumDictionaryKeys`                                                                                               |
| `setMaximumDictionarySize`              | `getMaximumDictionarySize`                                                                                               |
| `setOnWriteCompleted`                   | `onWriteCompleted`                                                                                                       |
| `setRowGroupInfo`                       | `getRowGroupInfo`, which returns a [`RowGroupInfo`](/core/javadoc/io/deephaven/parquet/table/metadata/RowGroupInfo.html) |
| `setSpecialInstructions`                | `getSpecialInstructions`                                                                                                 |
| `setTableDefinition`                    | `getTableDefinition`                                                                                                     |
| `setTargetPageSize`                     | `getTargetPageSize`                                                                                                      |
| `setUnsignedLongTarget`                 | `getUnsignedLongTarget(columnName)`, which returns an empty `Optional` if no target was set                              |
| `useDictionary`                         | `useDictionary(columnName)`, which returns `false` if no hint was set                                                    |

The `ParquetInstructions` class also has the following methods:

- `builder`: Returns a new [`ParquetInstructions.Builder`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html) instance.
- `getColumnNameFromParquetColumnNameOrDefault(parquetColumnName)`: Returns the column name in the Deephaven table corresponding to the specified Parquet column name, or the Parquet column name if no mapping exists.
- `getParquetColumnNameFromColumnNameOrDefault(columnName)`: Returns the Parquet column name corresponding to the specified column name, or the column name if no mapping exists.
- `withLayout(fileLayout)`: Returns a new `ParquetInstructions` instance with the supplied [`ParquetFileLayout`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html).
- `withTableDefinition(tableDefinition)`: Returns a new `ParquetInstructions` instance with the supplied table definition.
- `withTableDefinitionAndLayout(tableDefinition, fileLayout)`: Returns a new `ParquetInstructions` instance with the supplied table definition and `ParquetFileLayout`.

## `S3Instructions`

To read Parquet files from or write them to S3, pass an [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html) instance to the `setSpecialInstructions` method of [`ParquetInstructions.Builder`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html). Create the instance with `S3Instructions.builder`, call the builder's methods to set options, and then call `build`:

```groovy order=null
import io.deephaven.extensions.s3.S3Instructions
import io.deephaven.extensions.s3.Credentials
import io.deephaven.parquet.table.ParquetInstructions

// create S3Instructions instance for a public bucket
s3Instructions = S3Instructions.builder()
    .regionName("us-east-1")
    .credentials(Credentials.anonymous())
    .build()

// pass s3Instructions to the ParquetInstructions builder
s3ParquetInstructions = ParquetInstructions.builder().setSpecialInstructions(s3Instructions).build()
```

For a complete example that reads from S3, see [Import Parquet files](./parquet-import.md#from-s3).

### `S3Instructions` methods

Set each of the following options with the [`S3Instructions.Builder`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.Builder.html) method of the same name, and read it back with the `S3Instructions` method of the same name:

- `configFilePath`: The path to the AWS configuration file used to configure the default region, credentials, and other settings. If this is not set, the AWS SDK picks the file from the `aws.configFile` system property, the `AWS_CONFIG_FILE` environment variable, or `{user.home}/.aws/config`.
- `connectionTimeout`: A `Duration` representing the amount of time to wait for a successful S3 connection before timing out. The default is 2 seconds.
- `credentials`: The [`Credentials`](/core/javadoc/io/deephaven/extensions/s3/Credentials.html) to use for reading and writing files. Options are:
  - `Credentials.resolving`: (default) Use profile credentials if a profile name, configuration file path, or credentials file path is set. Otherwise, look in the AWS SDK's default locations and fall back to anonymous credentials.
  - `Credentials.anonymous`: Use anonymous credentials.
  - `Credentials.basic(accessKeyId, secretAccessKey)`: Use basic credentials with the specified access key ID and secret access key.
  - `Credentials.defaultCredentials`: Use the AWS SDK's default credentials provider.
  - `Credentials.profile`: Use credentials from the AWS configuration and credentials files.
  - `Credentials.session(accessKeyId, secretAccessKey, sessionToken)`: Use temporary credentials, such as those from AWS STS.
- `credentialsFilePath`: The path to the AWS credentials file used to configure the default region, credentials, and other settings. If this is not set, the AWS SDK picks the file from the `aws.credentialsFile` system property, the `AWS_CREDENTIALS_FILE` environment variable, or `{user.home}/.aws/credentials`.
- `endpointOverride`: The endpoint to connect to. Set it when connecting to a non-AWS, S3-compatible API. Callers connecting to AWS don't typically need it. By default, no endpoint override is set.
- `fragmentSize`: The maximum byte size of each fragment to read from S3. The default is 65536, and the minimum is 8192.
- `maxConcurrentRequests`: The maximum number of concurrent requests to make to S3. The default is 256.
- `numConcurrentWriteParts`: The maximum number of parts that can upload concurrently when writing to S3 before the write blocks. The default is 64. The value can't exceed `maxConcurrentRequests`.
- `profileName`: The AWS profile used to configure the default region, credentials, and other settings. If this is not set, the AWS SDK picks the profile from the `aws.profile` system property, the `AWS_PROFILE` environment variable, or `default`.
- `readAheadCount`: The number of fragments to read asynchronously ahead of the fragment currently being read. The default is 32.
- `readTimeout`: The amount of time to wait for a fragment read to complete before timing out. The default is 2 seconds.
- `regionName`: The region name of the AWS S3 bucket where the Parquet data exists. If you don't set it, the AWS SDK picks the region from the first of these sources that provides one:
  - The `aws.region` system property.
  - The `AWS_REGION` environment variable.
  - The `{user.home}/.aws/credentials` or `{user.home}/.aws/config` file.
  - The EC2 metadata service, when running in EC2.

  If none of these sources provides a region, or the region is wrong for the bucket, Deephaven derives the correct region with one additional request.

- `writePartSize`: The size of each part (in bytes) to upload when writing to S3. The default is 10485760, and the minimum is 5242880.
- `writeTimeout`: The amount of time to wait for a fragment write to complete before timing out. The default is 2 seconds.

A built `S3Instructions` instance also has the `withEndpointOverride(endpointOverride)` method, which returns a copy of the instance with the supplied endpoint override.

## Related documentation

- [Supported Parquet formats](./parquet-formats.md)
- [Import Parquet files](./parquet-import.md)
- [Export Parquet files](./parquet-export.md)
