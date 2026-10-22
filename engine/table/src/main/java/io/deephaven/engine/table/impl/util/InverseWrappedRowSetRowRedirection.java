//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.jetbrains.annotations.NotNull;

public class InverseWrappedRowSetRowRedirection implements RowRedirection {

    /**
     * {@link RowSet} used to map from outer row key (row key in {@code wrappedRowSet}) to inner row key (row position
     * in {@code wrappedRowSet}).
     */
    private final RowSet wrappedRowSet;

    /**
     * {@code wrappedRowSet} as a {@link TrackingRowSet}, used to map previous row keys; null if it is not tracking.
     */
    private final TrackingRowSet trackingRowSet;

    /**
     * This class accepts a {@link RowSet} or a {@link TrackingRowSet}. Only a TrackingRowSet provides previous values,
     * so {@link #getPrev(long)} and {@link #fillPrevChunk(FillContext, WritableChunk, RowSequence)} must not be called
     * for a RowSet that is not tracking.
     *
     * @param wrappedRowSet the RowSet (or TrackingRowSet) to use as the redirection source
     */
    public InverseWrappedRowSetRowRedirection(final RowSet wrappedRowSet) {
        this.wrappedRowSet = wrappedRowSet;
        this.trackingRowSet = wrappedRowSet.isTracking() ? wrappedRowSet.trackingCast() : null;
    }

    @Override
    public boolean ascendingMapping() {
        return true;
    }

    @Override
    public long get(long key) {
        if (key < 0 || wrappedRowSet.lastRowKey() < key) {
            return RowSet.NULL_ROW_KEY;
        }
        return wrappedRowSet.find(key);
    }

    @Override
    public long getPrev(long key) {
        if (key < 0 || trackingRowSet.lastRowKeyPrev() < key) {
            return RowSet.NULL_ROW_KEY;
        }
        return trackingRowSet.findPrev(key);
    }

    @Override
    public void fillChunk(
            @NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> mappedKeysOut,
            @NotNull final RowSequence keysToMap) {
        doMapping(mappedKeysOut.asWritableLongChunk(), keysToMap, wrappedRowSet);
    }

    @Override
    public void fillPrevChunk(
            @NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> mappedKeysOut,
            @NotNull final RowSequence keysToMap) {
        doMapping(mappedKeysOut.asWritableLongChunk(), keysToMap, trackingRowSet.prev());
    }

    /**
     * Fill {@code mappedKeysOut} with the positions of {@code keysToMap}, all of which must be in {@code rowSet}.
     */
    private static void doMapping(
            @NotNull final WritableLongChunk<? super RowKeys> mappedKeysOut,
            @NotNull final RowSequence keysToMap,
            @NotNull final RowSet rowSet) {
        final int size = keysToMap.intSize();
        if (size == 0) {
            mappedKeysOut.setSize(0);
            return;
        }
        final long firstPosition = rowSet.find(keysToMap.firstRowKey());
        final long lastPosition = rowSet.find(keysToMap.lastRowKey());
        if (firstPosition >= 0 && lastPosition - firstPosition + 1 == size) {
            // rowSet has exactly as many keys from first to last as keysToMap, so keysToMap is that slice of rowSet
            for (int ii = 0; ii < size; ++ii) {
                mappedKeysOut.set(ii, firstPosition + ii);
            }
            mappedKeysOut.setSize(size);
            return;
        }
        rowSet.invert(keysToMap.asRowKeyChunk(), mappedKeysOut);
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("{");

        long positionStart = 0;

        for (final RowSet.RangeIterator rangeIterator = wrappedRowSet.rangeIterator(); rangeIterator.hasNext();) {
            rangeIterator.next();

            if (positionStart > 0) {
                builder.append(", ");
            }
            final long rangeStart = rangeIterator.currentRangeStart();
            final long length = rangeIterator.currentRangeEnd() - rangeStart + 1;
            if (length > 1) {
                builder.append(rangeStart).append("-").append(rangeIterator.currentRangeEnd())
                        .append(" -> ").append(positionStart).append("-").append(positionStart + length - 1);
            } else {
                builder.append(rangeStart).append(" -> ").append(positionStart);
            }
            positionStart += length;
        }

        builder.append("}");

        return builder.toString();
    }
}
