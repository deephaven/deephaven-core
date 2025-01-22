//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import com.google.common.io.CountingOutputStream;
import io.deephaven.parquet.compress.CompressorAdapter;
import io.deephaven.parquet.impl.ParquetSchemaUtil;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

final class RowGroupWriterImpl implements RowGroupWriter {
    private final CountingOutputStream countingOutput;
    private final MessageType schema;
    private final int targetPageSize;
    private final ByteBufferAllocator allocator;
    private ColumnWriterImpl activeWriter;
    private final BlockMetaData blockMetaData;
    private final List<OffsetIndex> currentOffsetIndexes = new ArrayList<>();
    private final CompressorAdapter compressorAdapter;

    RowGroupWriterImpl(CountingOutputStream countingOutput,
            MessageType schema,
            int targetPageSize,
            ByteBufferAllocator allocator,
            CompressorAdapter compressorAdapter) {
        this(countingOutput, schema, targetPageSize, allocator, new BlockMetaData(), compressorAdapter);
    }


    private RowGroupWriterImpl(CountingOutputStream countingOutput,
            MessageType schema,
            int targetPageSize,
            ByteBufferAllocator allocator,
            BlockMetaData blockMetaData,
            CompressorAdapter compressorAdapter) {
        this.countingOutput = Objects.requireNonNull(countingOutput);
        this.schema = Objects.requireNonNull(schema);
        this.targetPageSize = targetPageSize;
        this.allocator = Objects.requireNonNull(allocator);
        this.blockMetaData = Objects.requireNonNull(blockMetaData);
        this.compressorAdapter = Objects.requireNonNull(compressorAdapter);
    }

    String[] getPrimitivePath(String columnName) {
        String[] result = {columnName};

        Type rollingType;
        while (!(rollingType = schema.getType(result)).isPrimitive()) {
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
                countingOutput,
                ParquetSchemaUtil.columnDescriptor(schema, getPrimitivePath(columnName)).orElseThrow(),
                compressorAdapter,
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
