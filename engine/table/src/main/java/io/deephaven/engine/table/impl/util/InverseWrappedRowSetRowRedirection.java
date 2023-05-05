/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

public class InverseWrappedRowSetRowRedirection implements RowRedirection {

    /**
     * {@link RowSet} used to map from outer row key (row key in {@code wrappedRowSet}) to inner row key (row position
     * in {@code wrappedRowSet}).
     */
    private final RowSet wrappedRowSet;

    /**
     * This class accepts a {@link RowSet} and attempts to cast to a {@link TrackingRowSet} if {@link #getPrev(long)} or
     * {@link #fillPrevChunk(FillContext, WritableChunk, RowSequence)} is called. Calling these functions on a
     * non-tracking RowSet will result in a {@link ClassCastException}.
     *
     * @param wrappedRowSet the RowSet (or TrackingRowSet) to use as the redirection source
     */
    public InverseWrappedRowSetRowRedirection(final RowSet wrappedRowSet) {
        this.wrappedRowSet = wrappedRowSet;
    }

    @Override
    public boolean ascendingMapping() {
        return true;
    }

    @Override
    public synchronized long get(long key) {
        if (key < 0 || key > wrappedRowSet.lastRowKey()) {
            return RowSet.NULL_ROW_KEY;
        }
        return wrappedRowSet.find(key);
    }

    @Override
    public synchronized long getPrev(long key) {
        if (key < 0 || key > wrappedRowSet.trackingCast().lastRowKeyPrev()) {
            return RowSet.NULL_ROW_KEY;
        }
        return wrappedRowSet.trackingCast().findPrev(key);
    }

    @Override
    public void fillChunk(
            @NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> mappedKeysOut,
            @NotNull final RowSequence keysToMap) {
        final WritableLongChunk<? super RowKeys> mappedKeysOutTyped = mappedKeysOut.asWritableLongChunk();
        try (final RowSequence.Iterator rsIt = wrappedRowSet.getRowSequenceIterator()) {
            doMapping(mappedKeysOutTyped, keysToMap, rsIt);
        }
    }

    @Override
    public void fillPrevChunk(
            @NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> mappedKeysOut,
            @NotNull final RowSequence keysToMap) {
        final WritableLongChunk<? super RowKeys> mappedKeysOutTyped = mappedKeysOut.asWritableLongChunk();
        try (final RowSet prevWrappedRowSet = wrappedRowSet.trackingCast().copyPrev();
                final RowSequence.Iterator prevOkIt = prevWrappedRowSet.getRowSequenceIterator()) {
            doMapping(mappedKeysOutTyped, keysToMap, prevOkIt);
        }
    }

    private void doMapping(
            @NotNull final WritableLongChunk<? super RowKeys> mappedKeysOut,
            @NotNull final RowSequence keysToMap,
            @NotNull final RowSequence.Iterator rsIt) {
        final MutableLong currentPosition = new MutableLong(0);
        mappedKeysOut.setSize(0);
        keysToMap.forEachRowKeyRange((start, end) -> {
            final long positionDelta = rsIt.advanceAndGetPositionDistance(start);
            final long rangeStartPosition = currentPosition.addAndGet(positionDelta);
            // handle start to end - 1, where we must increment
            for (long keyToMap = start; keyToMap <= end; ++keyToMap) {
                mappedKeysOut.add(rangeStartPosition + (keyToMap - start));
            }
            return true;
        });
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
