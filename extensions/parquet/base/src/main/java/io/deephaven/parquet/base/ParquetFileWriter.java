//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import com.google.common.io.CountingOutputStream;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import io.deephaven.parquet.compress.CompressorAdapter;
import io.deephaven.parquet.compress.DeephavenCompressorAdapterFactory;
import org.apache.parquet.Version;
import org.apache.parquet.bytes.ByteBufferAllocator;

import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.schema.MessageType;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.deephaven.parquet.base.ParquetUtils.MAGIC;
import static org.apache.parquet.format.Util.writeFileMetaData;

public final class ParquetFileWriter implements AutoCloseable {
    private static final ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter();
    private static final int VERSION = 1;

    private final CountingOutputStream countingOutput;
    private final MessageType type;
    private final int targetPageSize;
    private final ByteBufferAllocator allocator;
    private final CompressorAdapter compressorAdapter;
    private final Map<String, String> extraMetaData;
    private final List<BlockMetaData> blocks = new ArrayList<>();
    private final List<List<OffsetIndex>> offsetIndexes = new ArrayList<>();
    private final URI dest;
    private final ParquetMetadataFileWriter metadataFileWriter;

    public ParquetFileWriter(
            final URI dest,
            final OutputStream destOutputStream,
            final int targetPageSize,
            final ByteBufferAllocator allocator,
            final MessageType type,
            final String codecName,
            final Map<String, String> extraMetaData,
            @NotNull final ParquetMetadataFileWriter metadataFileWriter) throws IOException {
        this.targetPageSize = targetPageSize;
        this.allocator = allocator;
        this.extraMetaData = new HashMap<>(extraMetaData);
        this.countingOutput = new CountingOutputStream(destOutputStream);
        countingOutput.write(MAGIC);
        this.type = type;
        this.compressorAdapter = DeephavenCompressorAdapterFactory.getInstance().getByName(codecName);
        this.dest = dest;
        this.metadataFileWriter = metadataFileWriter;
    }

    public RowGroupWriter addRowGroup(final long size) {
        final RowGroupWriterImpl rowGroupWriter =
                new RowGroupWriterImpl(countingOutput, type, targetPageSize, allocator, compressorAdapter);
        rowGroupWriter.getBlock().setRowCount(size);
        blocks.add(rowGroupWriter.getBlock());
        offsetIndexes.add(rowGroupWriter.offsetIndexes());
        return rowGroupWriter;
    }

    /**
     * Get the number of bytes written to the parquet file so far.
     */
    public long bytesWritten() {
        return countingOutput.getCount();
    }

    @Override
    public void close() throws IOException {
        serializeOffsetIndexes();
        final ParquetMetadata footer =
                new ParquetMetadata(new FileMetaData(type, extraMetaData, Version.FULL_VERSION), blocks);
        serializeFooter(footer, countingOutput);
        metadataFileWriter.addParquetFileMetadata(dest, footer);
        // Flush any buffered data, do not close the stream since it is managed by the layer above
        countingOutput.flush();
        compressorAdapter.close();
    }

    public static void serializeFooter(final ParquetMetadata footer, final CountingOutputStream countingOutput)
            throws IOException {
        final long footerIndex = countingOutput.getCount();
        final org.apache.parquet.format.FileMetaData parquetMetadata =
                metadataConverter.toParquetMetadata(VERSION, footer);
        writeFileMetaData(parquetMetadata, countingOutput);
        countingOutput.write(intToLittleEndian((int) (countingOutput.getCount() - footerIndex)));
        countingOutput.write(MAGIC);
    }

    private static byte[] intToLittleEndian(final int value) {
        return new byte[] {
                (byte) (value & 0xFF),
                (byte) ((value >> 8) & 0xFF),
                (byte) ((value >> 16) & 0xFF),
                (byte) ((value >> 24) & 0xFF)
        };
    }

    private void serializeOffsetIndexes() throws IOException {
        for (int bIndex = 0, bSize = blocks.size(); bIndex < bSize; ++bIndex) {
            final List<ColumnChunkMetaData> columns = blocks.get(bIndex).getColumns();
            final List<OffsetIndex> blockOffsetIndexes = offsetIndexes.get(bIndex);
            for (int cIndex = 0, cSize = columns.size(); cIndex < cSize; ++cIndex) {
                final OffsetIndex offsetIndex = blockOffsetIndexes.get(cIndex);
                if (offsetIndex == null) {
                    continue;
                }
                final ColumnChunkMetaData column = columns.get(cIndex);
                final long offset = countingOutput.getCount();
                Util.writeOffsetIndex(ParquetMetadataConverter.toParquetOffsetIndex(offsetIndex), countingOutput);
                column.setOffsetIndexReference(
                        new IndexReference(offset, (int) (countingOutput.getCount() - offset)));
            }
        }
    }
}
