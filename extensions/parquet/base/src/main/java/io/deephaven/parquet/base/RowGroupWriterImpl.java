/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base;

import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import io.deephaven.parquet.compress.Compressor;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RowGroupWriterImpl implements RowGroupWriter {
    private final SeekableByteChannel writeChannel;
    private final MessageType type;
    private final int targetPageSize;
    private final ByteBufferAllocator allocator;
    private ColumnWriterImpl activeWriter;
    private final BlockMetaData blockMetaData;
    private final List<OffsetIndex> currentOffsetIndexes = new ArrayList<>();
    private final Compressor compressor;

    RowGroupWriterImpl(String path,
            boolean append,
            SeekableChannelsProvider channelsProvider,
            MessageType type,
            int targetPageSize,
            ByteBufferAllocator allocator,
            Compressor compressor)
            throws IOException {
        this(channelsProvider.getWriteChannel(path, append), type, targetPageSize, allocator, blockWithPath(path),
                compressor);
    }

    private static BlockMetaData blockWithPath(String path) {
        BlockMetaData blockMetaData = new BlockMetaData();
        blockMetaData.setPath(path);
        return blockMetaData;
    }

    RowGroupWriterImpl(SeekableByteChannel writeChannel,
            MessageType type,
            int targetPageSize,
            ByteBufferAllocator allocator,
            Compressor compressor) {
        this(writeChannel, type, targetPageSize, allocator, new BlockMetaData(), compressor);
    }


    private RowGroupWriterImpl(SeekableByteChannel writeChannel,
            MessageType type,
            int targetPageSize,
            ByteBufferAllocator allocator,
            BlockMetaData blockMetaData,
            Compressor compressor) {
        this.writeChannel = writeChannel;
        this.type = type;
        this.targetPageSize = targetPageSize;
        this.allocator = allocator;
        this.blockMetaData = blockMetaData;
        this.compressor = compressor;
    }

    String[] getPrimitivePath(String columnName) {
        String[] result = {columnName};

        Type rollingType;
        while (!(rollingType = type.getType(result)).isPrimitive()) {
            GroupType groupType = rollingType.asGroupType();
            if (groupType.getFieldCount() != 1) {
                throw new UnsupportedOperationException("Encountered struct at:" + Arrays.toString(result));
            }
            result = Arrays.copyOf(result, result.length + 1);
            result[result.length - 1] = groupType.getFieldName(0);
        }
        return result;
    }

    @Override
    public ColumnWriter addColumn(String columnName) {
        if (activeWriter != null) {
            throw new IllegalStateException(
                    "There is already an active column writer for " + activeWriter.getColumn().getPath()[0]
                            + " need to close that before opening a writer for " + columnName);
        }
        activeWriter = new ColumnWriterImpl(this,
                writeChannel,
                type.getColumnDescription(getPrimitivePath(columnName)),
                compressor,
                targetPageSize,
                allocator);
        return activeWriter;
    }

    @Override
    public BlockMetaData getBlock() {
        return blockMetaData;
    }

    void releaseWriter(ColumnWriterImpl columnWriter, ColumnChunkMetaData columnChunkMetaData) {
        if (activeWriter != columnWriter) {
            throw new RuntimeException(columnWriter.getColumn().getPath()[0] + " is not the active column");
        }
        currentOffsetIndexes.add(columnWriter.getOffsetIndex());
        blockMetaData.addColumn(columnChunkMetaData);
        blockMetaData.setTotalByteSize(columnChunkMetaData.getTotalSize() + blockMetaData.getTotalByteSize());
        activeWriter = null;
    }

    List<OffsetIndex> offsetIndexes() {
        return currentOffsetIndexes;
    }
}
