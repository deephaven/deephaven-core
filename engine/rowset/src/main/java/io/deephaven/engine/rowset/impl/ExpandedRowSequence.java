/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.rowset.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
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
 * Note that this is a very implementation-specific tool, and may not support all usual RowSequence methods. Notably,
 * it does not support getting sub-RowSequences, RowSequence.Iterators, or conversion to RowSet.
 */
public class ExpandedRowSequence extends RowSequenceAsChunkImpl implements RowSequence {

    public static RowSequence expand(@NotNull final RowSequence innerRowSequence, final int multiplier) {
        Require.gtZero(multiplier, "multiplier");
        if (innerRowSequence instanceof ExpandedRowSequence) {
            final ExpandedRowSequence innerExpanded = ((ExpandedRowSequence) innerRowSequence);
            return expand(innerExpanded.innerRowSequence, innerExpanded.multiplier * multiplier);
        }
        return multiplier == 1 ? innerRowSequence : new ExpandedRowSequence(innerRowSequence, multiplier);
    }

    private RowSequence innerRowSequence;
    private int multiplier;

    private ExpandedRowSequence(@NotNull final RowSequence innerRowSequence, final int multiplier) {
        Assert.assertion(!(innerRowSequence instanceof ExpandedRowSequence),
                "innerRowSequence must not be an ExpandedRowSequence");
        this.innerRowSequence = innerRowSequence;
        this.multiplier = multiplier;
    }

    public ExpandedRowSequence() {
        innerRowSequence = null;
        multiplier = 0;
    }

    public RowSequence reset(@NotNull final RowSequence innerRowSequence, final int multiplier) {
        if (innerRowSequence instanceof ExpandedRowSequence) {
            final ExpandedRowSequence innerExpanded = ((ExpandedRowSequence) innerRowSequence);
            this.innerRowSequence = innerExpanded.innerRowSequence;
            this.multiplier = innerExpanded.multiplier * multiplier;
        } else {
            this.innerRowSequence = innerRowSequence;
            this.multiplier = multiplier;
        }
        invalidateRowSequenceAsChunkImpl();
        return this;
    }

    public final void clear() {
        innerRowSequence = null;
        multiplier = 0;
        invalidateRowSequenceAsChunkImpl();
    }

    @Override
    public Iterator getRowSequenceIterator() {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public void fillRowKeyChunk(@NotNull final WritableLongChunk<? super OrderedRowKeys> chunkToFill) {
        chunkToFill.setSize(0);
        innerRowSequence.forAllRowKeyRanges((s, e) -> {
            final long last = e * multiplier + multiplier - 1;
            for (long next = s * multiplier; next <= last; ++next) {
                chunkToFill.add(next);
            }
        });
    }

    @Override
    public void fillRowKeyRangesChunk(@NotNull final WritableLongChunk<OrderedRowKeyRanges> chunkToFill) {
        chunkToFill.setSize(0);
        innerRowSequence.forAllRowKeyRanges((s, e) -> {
            chunkToFill.add(s * multiplier);
            chunkToFill.add(e * multiplier + multiplier - 1);
        });
    }

    @Override
    public boolean isEmpty() {
        return innerRowSequence.isEmpty();
    }

    @Override
    public long firstRowKey() {
        return innerRowSequence.firstRowKey() * multiplier;
    }

    @Override
    public long lastRowKey() {
        return innerRowSequence.lastRowKey() * multiplier + multiplier - 1;
    }

    @Override
    public long size() {
        return innerRowSequence.size() * multiplier;
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
