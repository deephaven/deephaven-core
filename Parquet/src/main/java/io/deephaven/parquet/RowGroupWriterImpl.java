package io.deephaven.parquet;

import io.deephaven.parquet.utils.SeekableChannelsProvider;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory;
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
    private final int pageSize;
    private final ByteBufferAllocator allocator;
    private ColumnWriterImpl activeWriter;
    private final BlockMetaData blockMetaData;
    private final List<OffsetIndex> currentOffsetIndexes = new ArrayList<>();
    private final CompressionCodecFactory.BytesInputCompressor compressor;

    RowGroupWriterImpl(String path, boolean append, SeekableChannelsProvider channelsProvider, MessageType type,
            int pageSize, ByteBufferAllocator allocator, CompressionCodecFactory.BytesInputCompressor compressor)
            throws IOException {
        this(channelsProvider.getWriteChannel(path, append), type, pageSize, allocator, blockWithPath(path),
                compressor);
    }

    private static BlockMetaData blockWithPath(String path) {
        BlockMetaData blockMetaData = new BlockMetaData();
        blockMetaData.setPath(path);
        return blockMetaData;
    }

    RowGroupWriterImpl(SeekableByteChannel writeChannel, MessageType type, int pageSize, ByteBufferAllocator allocator,
            CompressionCodecFactory.BytesInputCompressor compressor) {
        this(writeChannel, type, pageSize, allocator, new BlockMetaData(), compressor);
    }


    private RowGroupWriterImpl(SeekableByteChannel writeChannel, MessageType type, int pageSize,
            ByteBufferAllocator allocator, BlockMetaData blockMetaData,
            CompressionCodecFactory.BytesInputCompressor compressor) {
        this.writeChannel = writeChannel;
        this.type = type;
        this.pageSize = pageSize;
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
            throw new RuntimeException(
                    "There is already an active column writer for " + activeWriter.getColumn().getPath()[0]
                            + " need to close that before opening a writer for " + columnName);
        }
        activeWriter = new ColumnWriterImpl(this, writeChannel, type.getColumnDescription(getPrimitivePath(columnName)),
                compressor, pageSize, allocator);
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
