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

public class InverseRowRedirectionImpl implements WritableRowRedirection {

    /**
     * {@link RowSet} used to map from outer key (position in the index) to inner key.
     */
    private final TrackingRowSet wrappedIndex;

    public InverseRowRedirectionImpl(final TrackingRowSet wrappedIndex) {
        this.wrappedIndex = wrappedIndex;
    }

    @Override
    public boolean ascendingMapping() {
        return true;
    }

    @Override
    public synchronized long put(long key, long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized long get(long key) {
        if (key < 0 || key > wrappedIndex.lastRowKey()) {
            return RowSet.NULL_ROW_KEY;
        }
        return wrappedIndex.find(key);
    }

    @Override
    public synchronized long getPrev(long key) {
        if (key < 0 || key > wrappedIndex.lastRowKeyPrev()) {
            return RowSet.NULL_ROW_KEY;
        }
        return wrappedIndex.findPrev(key);
    }

    @Override
    public void fillChunk(@NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> mappedKeysOut,
            @NotNull final RowSequence keysToMap) {
        final WritableLongChunk<? super RowKeys> mappedKeysOutTyped = mappedKeysOut.asWritableLongChunk();
        try (final RowSequence.Iterator okit = wrappedIndex.getRowSequenceIterator()) {
            doMapping(mappedKeysOutTyped, keysToMap, okit);
        }
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> mappedKeysOut,
            @NotNull final RowSequence keysToMap) {
        final WritableLongChunk<? super RowKeys> mappedKeysOutTyped = mappedKeysOut.asWritableLongChunk();
        try (final RowSet prevWrappedIndex = wrappedIndex.copyPrev();
                final RowSequence.Iterator prevOkIt = prevWrappedIndex.getRowSequenceIterator()) {
            doMapping(mappedKeysOutTyped, keysToMap, prevOkIt);
        }
    }

    private void doMapping(@NotNull WritableLongChunk<? super RowKeys> mappedKeysOut, @NotNull RowSequence keysToMap,
            RowSequence.Iterator okit) {
        final MutableLong currentPosition = new MutableLong(0);
        mappedKeysOut.setSize(0);
        keysToMap.forEachRowKeyRange((start, end) -> {
            final long positionDelta = okit.advanceAndGetPositionDistance(start);
            final long rangeStartPosition = currentPosition.addAndGet(positionDelta);
            // handle start to end - 1, where we must increment
            for (long keyToMap = start; keyToMap <= end; ++keyToMap) {
                mappedKeysOut.add(rangeStartPosition + (keyToMap - start));
            }
            return true;
        });
    }

    @Override
    public void startTrackingPrevValues() {
        // Deliberately left blank. Nothing to do here.
    }

    @Override
    public synchronized long remove(long leftIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("{");

        long positionStart = 0;

        for (final RowSet.RangeIterator rangeIterator = wrappedIndex.rangeIterator(); rangeIterator.hasNext();) {
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
