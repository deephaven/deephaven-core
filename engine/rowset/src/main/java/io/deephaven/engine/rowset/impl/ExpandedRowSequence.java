/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.rowset.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.util.datastructures.LongAbortableConsumer;
import io.deephaven.util.datastructures.LongRangeAbortableConsumer;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

/**
 * Expand each row key in a {@link RowSequence} {@code innerRowSequence} according to a {@code multiplier}, such that
 * each inner key {@code k} is mapped to {@code multiplier} outer row keys in range
 * {@code [k * multiplier, k * multiplier + multiplier)}.
 * <p>
 * Note that this is a very implementation-specific tool, and may not support all usual RowSequence methods. Notably, it
 * does not support getting sub-RowSequences, RowSequence.Iterators, or conversion to RowSet.
 */
public class ExpandedRowSequence extends RowSequenceAsChunkImpl implements RowSequence {

    public static RowSequence expand(@NotNull final RowSequence innerRowSequence, final int multiplier) {
        Require.gtZero(multiplier, "multiplier");
        // noinspection resource
        return multiplier == 1
                ? innerRowSequence
                : new ExpandedRowSequence().reset(innerRowSequence, multiplier, 0,
                        innerRowSequence.size() * multiplier);
    }

    private RowSequence innerRowSequence;
    private int multiplier;
    private long offset;
    private long size;

    /**
     * Make a reusable ExpandedRowSequence, to be paired with {@link #reset(RowSequence, int, long, long)}.
     */
    public ExpandedRowSequence() {}

    /**
     * Reset the contexts of this ExpandedRowSequence.
     *
     * @param innerRowSequence The new inner row sequence; must not be an ExpandedRowSequence
     * @param multiplier The new multiplier
     * @param offset Offset into the expanded space where this RowSequence begins
     * @param size Total size of the expanded space after the offset
     * @return {@code this}
     */
    public RowSequence reset(
            @NotNull final RowSequence innerRowSequence,
            final int multiplier,
            final long offset,
            final long size) {
        Require.leq(size, "size", innerRowSequence.size() * multiplier, "innerRowSequence.size() * multiplier");
        if (innerRowSequence instanceof ExpandedRowSequence) {
            final ExpandedRowSequence innerExpanded = ((ExpandedRowSequence) innerRowSequence);
            this.innerRowSequence = innerExpanded.innerRowSequence;
            this.multiplier = innerExpanded.multiplier * multiplier;
            this.offset = innerExpanded.offset * multiplier + offset;
        } else {
            this.innerRowSequence = innerRowSequence;
            this.multiplier = multiplier;
            this.offset = offset;
        }
        // TODO: Maybe separate concerns, and create a slice row sequence?
        this.size = size;
        invalidateRowSequenceAsChunkImpl();
        return this;
    }

    public final void clear() {
        innerRowSequence = null;
        multiplier = 0;
        offset = 0;
        size = 0;
        invalidateRowSequenceAsChunkImpl();
    }

    @Override
    public Iterator getRowSequenceIterator() {
        return new Iterator();
    }

    private class Iterator implements RowSequence.Iterator {

        private final ExpandedRowSequence expandedSlice = new ExpandedRowSequence();

        @Override
        public boolean hasMore() {
            return false;
        }

        @Override
        public long peekNextKey() {
            return 0;
        }

        @Override
        public RowSequence getNextRowSequenceThrough(final long maxKeyInclusive) {
            return null;
        }

        @Override
        public RowSequence getNextRowSequenceWithLength(final long numberOfKeys) {
            return null;
        }

        @Override
        public boolean advance(final long nextKey) {
            return false;
        }

        @Override
        public long getRelativePosition() {
            return 0;
        }
    }

    @Override
    public RowSequence getRowSequenceByPosition(final long startPositionInclusive, final long length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowSequence getRowSequenceByKeyRange(final long startRowKeyInclusive, final long endRowKeyInclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowSet asRowSet() {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        forAllRowKeyRanges(builder::appendRange);
        return builder.build();
    }

    @Override
    public void fillRowKeyChunk(@NotNull final WritableLongChunk<? super OrderedRowKeys> chunkToFill) {
        chunkToFill.setSize(0);
        forAllRowKeys(chunkToFill::add);
    }

    @Override
    public void fillRowKeyRangesChunk(@NotNull final WritableLongChunk<OrderedRowKeyRanges> chunkToFill) {
        chunkToFill.setSize(0);
        forAllRowKeyRanges((s, e) -> {
            chunkToFill.add(s);
            chunkToFill.add(e);
        });
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public long firstRowKey() {
        return size == 0 ? NULL_ROW_KEY : innerRowSequence.firstRowKey() * multiplier;
    }

    @Override
    public long lastRowKey() {
        return innerRowSequence.get(size / multiplier) innerRowSequence.lastRowKey() * multiplier + multiplier - 1;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public long getAverageRunLengthEstimate() {
        return innerRowSequence.getAverageRunLengthEstimate() * multiplier;
    }

    @Override
    public boolean forEachRowKey(@NotNull final LongAbortableConsumer consumer) {
        return innerRowSequence.forEachRowKey((final long innerRowKey) -> {
            final long multiplied = innerRowKey * multiplier;
            for (int offset = 0; offset < multiplier; ++offset) {
                if (!consumer.accept(multiplied + offset)) {
                    return false;
                }
            }
            return true;
        });
    }

    @Override
    public boolean forEachRowKeyRange(@NotNull final LongRangeAbortableConsumer consumer) {
        return innerRowSequence.forEachRowKeyRange(
                (s, e) -> consumer.accept(s * multiplier, e * multiplier + multiplier - 1));
    }

    @Override
    public void close() {
        closeRowSequenceAsChunkImpl();
        clear();
    }

    @Override
    public long rangesCountUpperBound() {
        final MutableInt numRanges = new MutableInt(0);
        innerRowSequence.forAllRowKeyRanges((final long start, final long end) -> numRanges.increment());
        return numRanges.intValue();
    }
}
