/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base;

import io.deephaven.parquet.base.tempfix.ParquetMetadataConverter;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import io.deephaven.parquet.compress.Compressor;
import io.deephaven.parquet.compress.DeephavenCodecFactory;
import org.apache.parquet.Version;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesUtils;

import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.format.Util.writeFileMetaData;

public class ParquetFileWriter {
    private static final ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter();
    private static final int VERSION = 1;

    private final SeekableByteChannel writeChannel;
    private final MessageType type;
    private final int targetPageSize;
    private final ByteBufferAllocator allocator;
    private final SeekableChannelsProvider channelsProvider;
    private final Compressor compressor;
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
        writeChannel = channelsProvider.getWriteChannel(filePath, false); // TODO add support for appending
        writeChannel.write(ByteBuffer.wrap(ParquetFileReader.MAGIC));
        this.type = type;
        this.channelsProvider = channelsProvider;
        this.compressor = DeephavenCodecFactory.getInstance().getByName(codecName);
    }

    @SuppressWarnings("unused")
    RowGroupWriter addRowGroup(final String path, final boolean append) throws IOException {
        RowGroupWriterImpl rowGroupWriter =
                new RowGroupWriterImpl(path, append, channelsProvider, type, targetPageSize, allocator, compressor);
        blocks.add(rowGroupWriter.getBlock());
        return rowGroupWriter;
    }

    public RowGroupWriter addRowGroup(final long size) {
        RowGroupWriterImpl rowGroupWriter =
                new RowGroupWriterImpl(writeChannel, type, targetPageSize, allocator, compressor);
        rowGroupWriter.getBlock().setRowCount(size);
        blocks.add(rowGroupWriter.getBlock());
        offsetIndexes.add(rowGroupWriter.offsetIndexes());
        return rowGroupWriter;
    }

    public void close() throws IOException {
        try (final OutputStream os = Channels.newOutputStream(writeChannel)) {
            serializeOffsetIndexes(offsetIndexes, blocks, os);
            ParquetMetadata footer =
                    new ParquetMetadata(new FileMetaData(type, extraMetaData, Version.FULL_VERSION), blocks);
            serializeFooter(footer, os);
        }
        // os (and thus writeChannel) are closed at this point.
    }

    private void serializeFooter(final ParquetMetadata footer, final OutputStream os) throws IOException {
        final long footerIndex = writeChannel.position();
        org.apache.parquet.format.FileMetaData parquetMetadata = metadataConverter.toParquetMetadata(VERSION, footer);
        writeFileMetaData(parquetMetadata, os);
        BytesUtils.writeIntLittleEndian(os, (int) (writeChannel.position() - footerIndex));
        os.write(ParquetFileReader.MAGIC);
    }

    private void serializeOffsetIndexes(
            final List<List<OffsetIndex>> offsetIndexes,
            final List<BlockMetaData> blocks,
            final OutputStream os) throws IOException {
        for (int bIndex = 0, bSize = blocks.size(); bIndex < bSize; ++bIndex) {
            final List<ColumnChunkMetaData> columns = blocks.get(bIndex).getColumns();
            final List<OffsetIndex> blockOffsetIndexes = offsetIndexes.get(bIndex);
            for (int cIndex = 0, cSize = columns.size(); cIndex < cSize; ++cIndex) {
                OffsetIndex offsetIndex = blockOffsetIndexes.get(cIndex);
                if (offsetIndex == null) {
                    continue;
                }
                ColumnChunkMetaData column = columns.get(cIndex);
                final long offset = writeChannel.position();
                Util.writeOffsetIndex(ParquetMetadataConverter.toParquetOffsetIndex(offsetIndex), os);
                column.setOffsetIndexReference(new IndexReference(offset, (int) (writeChannel.position() - offset)));
            }
        }
    }
}
