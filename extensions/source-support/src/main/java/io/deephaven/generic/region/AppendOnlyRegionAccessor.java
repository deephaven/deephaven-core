//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.generic.region;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

/**
 * Minimal interface for an append-only flat data source, with contiguous rows from 0, inclusive, to {@link #size()
 * size}, exclusive.
 */
public interface AppendOnlyRegionAccessor<ATTR extends Any> extends LongSizedDataStructure {

    /**
     * Read a suffix of a page from this region into {@code destination}, beginning at chunk position 0. Should read
     * between {@code minimumSize} and {@code destination.capacity()} rows, and report the resulting size via
     * {@link WritableChunk#setSize(int) setSize}.
     *
     * @param firstRowPosition The first row position to read
     * @param minimumSize The minimum number of rows to read that will satisfy this request
     * @param destination The destination chunk to fill
     */
    void readChunkPage(long firstRowPosition, int minimumSize, @NotNull WritableChunk<ATTR> destination);
}
