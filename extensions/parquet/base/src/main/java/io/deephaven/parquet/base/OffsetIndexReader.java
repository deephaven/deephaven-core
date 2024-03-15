//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.util.channel.SeekableChannelContext;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

/**
 * Interface for reading the offset index for a column chunk.
 */
public interface OffsetIndexReader {

    /**
     * @param context The channel context to use for reading the offset index.
     * @return Reads, caches, and returns the offset index for a column chunk.
     * @throws UnsupportedOperationException If the offset index cannot be read from this source.
     */
    OffsetIndex getOffsetIndex(SeekableChannelContext context);

    /**
     * A null implementation of the offset index reader which always throws an exception when called.
     */
    OffsetIndexReader NULL = context -> {
        throw new UnsupportedOperationException("Cannot read offset index from this source.");
    };
}
