/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base;

import org.apache.parquet.format.converter.ParquetMetadataConverter;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import io.deephaven.parquet.compress.CompressorAdapter;
import io.deephaven.parquet.compress.DeephavenCompressorAdapterFactory;
import org.apache.parquet.Version;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesUtils;

import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.format.Util.writeFileMetaData;

public final class ParquetFileWriter {
    private static final ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter();
    private static final int VERSION = 1;
    private static final int OUTPUT_BUFFER_SIZE = 1 << 18;

    private final PositionedBufferedOutputStream bufferedOutput;
    private final MessageType type;
    private final int targetPageSize;
    private final ByteBufferAllocator allocator;
    private final CompressorAdapter compressorAdapter;
    private final Map<String, String> extraMetaData;
    private final List<BlockMetaData> blocks = new ArrayList<>();
    private final List<List<OffsetIndex>> offsetIndexes = new ArrayList<>();

    public ParquetFileWriter(
            final String filePath,
            final SeekableChannelsProvider channelsProvider,
            final int targetPageSize,
            final ByteBufferAllocator allocator,
            final MessageType type,
            final String codecName,
            final Map<String, String> extraMetaData) throws IOException {
        this.targetPageSize = targetPageSize;
        this.allocator = allocator;
        this.extraMetaData = new HashMap<>(extraMetaData);
        bufferedOutput = new PositionedBufferedOutputStream(channelsProvider.getWriteChannel(filePath, false),
                OUTPUT_BUFFER_SIZE);
        bufferedOutput.write(ParquetFileReader.MAGIC);
        this.type = type;
        this.compressorAdapter = DeephavenCompressorAdapterFactory.getInstance().getByName(codecName);
    }

    public RowGroupWriter addRowGroup(final long size) {
        final RowGroupWriterImpl rowGroupWriter =
                new RowGroupWriterImpl(bufferedOutput, type, targetPageSize, allocator, compressorAdapter);
        rowGroupWriter.getBlock().setRowCount(size);
        blocks.add(rowGroupWriter.getBlock());
        offsetIndexes.add(rowGroupWriter.offsetIndexes());
        return rowGroupWriter;
    }

    public void close() throws IOException {
        serializeOffsetIndexes();
        final ParquetMetadata footer =
                new ParquetMetadata(new FileMetaData(type, extraMetaData, Version.FULL_VERSION), blocks);
        serializeFooter(footer);
        // Flush any buffered data and close the channel
        bufferedOutput.close();
        compressorAdapter.close();
    }

    private void serializeFooter(final ParquetMetadata footer) throws IOException {
        final long footerIndex = bufferedOutput.position();
        final org.apache.parquet.format.FileMetaData parquetMetadata =
                metadataConverter.toParquetMetadata(VERSION, footer);
        writeFileMetaData(parquetMetadata, bufferedOutput);
        BytesUtils.writeIntLittleEndian(bufferedOutput, (int) (bufferedOutput.position() - footerIndex));
        bufferedOutput.write(ParquetFileReader.MAGIC);
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
                final long offset = bufferedOutput.position();
                Util.writeOffsetIndex(ParquetMetadataConverter.toParquetOffsetIndex(offsetIndex), bufferedOutput);
                column.setOffsetIndexReference(new IndexReference(offset, (int) (bufferedOutput.position() - offset)));
            }
        }
    }
}
