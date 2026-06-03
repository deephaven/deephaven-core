---
title: writeTable
---

The `writeTable` method will write a table to a standard Parquet file.

## Syntax

```groovy
writeTable(sourceTable, destPath)
writeTable(sourceTable, destPath, writeInstructions)
```

> [!NOTE]
> To write multiple tables at once, use [`writeTables`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#writeTables(io.deephaven.engine.table.Table%5B%5D,java.lang.String%5B%5D,io.deephaven.parquet.table.ParquetInstructions)):
>
> ```groovy
> writeTables(sourceTables[], destPaths[], writeInstructions)
> ```

## Parameters

<ParamTable>
<Param name="sourceTable" type="Table">

The table to write to file.

</Param>
<Param name="destPath" type="String">

Path name or URI of the file where the table will be stored. The file name should end with the `.parquet` extension. If the path includes non-existing directories, they are created. If there is an error, any intermediate directories previously created are removed; note this makes this method unsafe for concurrent use.

</Param>
<Param name="writeInstructions" type="ParquetInstructions" optional>

Instructions for customizations while writing. For simple compression, use predefined constants:

- `ParquetTools.SNAPPY`: Aims for high speed, and a reasonable amount of compression, based on Snappy compression format by [Google](https://github.com/google/snappy/blob/main/format_description.txt).
- `ParquetTools.UNCOMPRESSED`: The output will not be compressed.
- `ParquetTools.LZ4_RAW`: A codec based on the [LZ4 block format](https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md). Should always be used instead of `LZ4`.
- `ParquetTools.LZ4`: **Deprecated** Use `LZ4_RAW` instead.
- `ParquetTools.LZO`: Compression codec based on or interoperable with the [LZO compression library](https://www.oberhumer.com/opensource/lzo/).
- `ParquetTools.GZIP`: Compression codec based on the GZIP format defined by [RFC 1952](https://tools.ietf.org/html/rfc1952).
- `ParquetTools.ZSTD`: Compression codec with a high compression ratio based on the Zstandard format defined by [RFC 8478](https://tools.ietf.org/html/rfc8478).
- `ParquetTools.BROTLI`: Compression codec based on [Brotli](https://github.com/google/brotli), offering high compression ratios.

If not specified, defaults to `SNAPPY`.

For advanced options such as Row Group configuration, metadata file generation, index columns, and S3 support, use `ParquetInstructions.builder()`. See the [Parquet instructions](../../../how-to-guides/data-import-export/parquet-instructions.md) guide for details.

</Param>
</ParamTable>

## Returns

A Parquet file located in the specified path.

## Examples

> [!NOTE]
> All examples in this document write data to the `/data` directory in Deephaven. For more information on this directory and how it relates to your local file system, see [Docker data volumes](../../../conceptual/docker-data-volumes.md).

### Single Parquet file

In this example, `writeTable` writes the source table to `/data/output.parquet`.

```groovy
import io.deephaven.parquet.table.ParquetTools

source = newTable(
    stringCol("X", "A", "B",  "B", "C", "B", "A", "B", "B", "C"),
    intCol("Y",2, 4, 2, 1, 2, 3, 4, 2, 3),
    intCol("Z", 55, 76, 20, 4, 230, 50, 73, 137, 214),
)

ParquetTools.writeTable(source, "/data/output.parquet")
```

### Compression codec

In this example, `writeTable` writes the source table `/data/output_GZIP.parquet` with `GZIP` compression.

```groovy
import io.deephaven.parquet.table.ParquetTools

source = newTable(
    stringCol("X", "A", "B",  "B", "C", "B", "A", "B", "B", "C"),
    intCol("Y",2, 4, 2, 1, 2, 3, 4, 2, 3),
    intCol("Z", 55, 76, 20, 4, 230, 50, 73, 137, 214),
)

ParquetTools.writeTable(source, "/data/output_GZIP.parquet", ParquetTools.GZIP)
```

## Related documentation

- [Import Parquet into Deephaven video](https://youtu.be/k4gI6hSZ2Jc)
- [Read Parquet files](../../../how-to-guides/data-import-export/parquet-import.md)
- [Write Parquet files](../../../how-to-guides/data-import-export/parquet-export.md)
- [`readTable`](./readTable.md)
- [`writeKeyValuePartitionedTable`](./writeKeyValuePartitionedTable.md)
- [Docker data volumes](../../../conceptual/docker-data-volumes.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#writeTable(io.deephaven.engine.table.Table,java.lang.String))
