//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;


import io.deephaven.util.channel.SeekableChannelContext;
import org.apache.parquet.column.Dictionary;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.IntBuffer;

/**
 * Provides the API for reading a single parquet page
 */
public interface ColumnPageReader extends AutoCloseable {

    /**
     * @param channelContext The channel context to use for reading the parquet file
     * @return The number of rows in this page, or -1 if it's unknown.
     */
    default long numRows(final SeekableChannelContext channelContext) throws IOException {
        return numValues();
    }

    /**
     * Triggers the value decompression and decoding
     * 
     * @param nullValue The value to be stored under the null entries
     * @param channelContext The channel context to use for reading the parquet file
     * @return the data for that page in a format that makes sense for the given type - typically array of something
     *         that makes sense
     */
    Object materialize(Object nullValue, SeekableChannelContext channelContext) throws IOException;

    /**
     * Directly read the key integral values when there's a dictionary.
     *
     * @param keyDest A properly sized buffer (at least numValues()) to hold the keys
     * @param nullPlaceholder The value to use for nulls.
     * @param channelContext The channel context to use for reading the parquet file
     *
     * @return A buffer holding the end of each repeated row. If the column is not repeating, null.
     */
    IntBuffer readKeyValues(IntBuffer keyDest, int nullPlaceholder,
            SeekableChannelContext channelContext) throws IOException;

    /**
     * @return The number of values in this page
     */
    int numValues();

    /**
     * @param channelContext The channel context to use for reading the parquet file
     * @return Parquet dictionary for this column chunk
     * @apiNote The result will never be {@code null}. It will instead be {@link ColumnChunkReader#NULL_DICTIONARY}.
     */
    @NotNull
    Dictionary getDictionary(SeekableChannelContext channelContext);
}
