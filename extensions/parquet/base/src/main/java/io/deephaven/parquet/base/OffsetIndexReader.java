package io.deephaven.parquet.base;

import io.deephaven.util.channel.SeekableChannelContext;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

/**
 * Interface for reading the offset index for a column chunk.
 */
public interface OffsetIndexReader {

    /**
     * Get the offset index for a column chunk.
     *
     * @param context The channel context to use for reading the offset index.
     */
    OffsetIndex getOffsetIndex(SeekableChannelContext context);

    /**
     * A null implementation of the offset index reader.
     */
    OffsetIndexReader NULL = context -> null;
}
