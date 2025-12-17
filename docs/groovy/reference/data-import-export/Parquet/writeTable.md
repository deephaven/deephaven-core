---
title: writeTable
---

The `writeTable` method will write a table to a standard Parquet file.

## Syntax

```
writeTable(sourceTable, destPath)
writeTable(sourceTable, destFile)
writeTable(sourceTable, destFile, definition)
writeTable(sourceTable, destFile, writeInstructions)
writeTable(sourceTable, destPath, definition, writeInstructions)
writeTable(sourceTable, destFile, definition, writeInstructions)
```

## Parameters

<ParamTable>
<Param name="sourceTable" type="Table">

The table to write to file.

</Param>
<Param name="destPath" type="String">

Path name of the file where the table will be stored. The file name should end with the `.parquet` extension. If the path includes non-existing directories, they are created.

</Param>
<Param name="destFile" type="File">

Destination file. Its path must end in ".parquet". Any non-existing directories in the path are created. If there is an error, any intermediate directories previously created are removed. Note: this makes this method unsafe for concurrent use.

</Param>
<Param name="definition" type="TableDefinition">

Table definition to use (instead of the one implied by the table itself).

</Param>
<Param name="writeInstructions" type="ParquetInstructions">

Instructions for customizations while writing. Valid values are:

- `ParquetTools.SNAPPY`: Aims for high speed, and a reasonable amount of compression, based on Snappy compression format by [Google](https://github.com/google/snappy/blob/main/format_description.txt).
- `ParquetTools.UNCOMPRESSED`: The output will not be compressed.
- `ParquetTools.LZ4_RAW`: Compression codec loosely based on the [LZ4 compression algorithm](https://github.com/lz4/lz4), but with an additional undocumented framing scheme. The framing is part of the original Hadoop compression library and was historically copied first in parquet-mr, then emulated with mixed results by parquet-cpp. Note that `LZ4` is not recommended for use with Parquet files. Use `LZ4_RAW` instead.
- `ParquetTools.LZ4`: **Deprecated** Compression codec loosely based on the [LZ4 compression algorithm](https://github.com/lz4/lz4), but with an additional undocumented framing scheme. The framing is part of the original Hadoop compression library and was historically copied first in parquet-mr, then emulated with mixed results by parquet-cpp. Note that `LZ4` is deprecated; use `LZ4_RAW` instead.
- `ParquetTools.LZO`: Compression codec based on or interoperable with the [LZO compression library](https://www.oberhumer.com/opensource/lzo/).
- `ParquetTools.GZIP`: Compression codec based on the GZIP format (not the closely-related "zlib" or "deflate" formats) defined by [RFC 1952](https://tools.ietf.org/html/rfc1952).
- `ParquetTools.ZSTD`: Compression codec with the highest compression ratio based on the Zstandard format defined by [RFC 8478](https://tools.ietf.org/html/rfc8478).

If not specified, defaults to `SNAPPY`.

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
- [Docker data volumes](../../../conceptual/docker-data-volumes.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#writeTable(io.deephaven.engine.table.Table,java.lang.String))
