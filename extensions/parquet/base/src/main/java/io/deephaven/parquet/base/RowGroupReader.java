//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.util.channel.SeekableChannelContext;
import org.apache.parquet.format.RowGroup;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Provides read access to a parquet Row Group
 */
public interface RowGroupReader {
    /**
     * Returns the accessor to a given Column Chunk
     * 
     * @param path the full column path
     * @param channelContext the channel context to use while reading the parquet file
     * @return the accessor to a given Column Chunk
     */
    ColumnChunkReader getColumnChunk(@NotNull List<String> path, @NotNull final SeekableChannelContext channelContext);

    long numRows();

    RowGroup getRowGroup();
}
