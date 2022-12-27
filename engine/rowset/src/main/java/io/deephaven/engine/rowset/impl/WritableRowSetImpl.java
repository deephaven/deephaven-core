/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.rowset.impl;

import gnu.trove.list.array.TLongArrayList;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.*;
import io.deephaven.util.datastructures.LongAbortableConsumer;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.util.datastructures.LongRangeAbortableConsumer;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.function.LongConsumer;

public class WritableRowSetImpl extends RowSequenceAsChunkImpl implements WritableRowSet, Externalizable {

    private static final long serialVersionUID = 1L;

    private OrderedLongSet innerSet;

    @SuppressWarnings("WeakerAccess") // Mandatory for Externalizable
    public WritableRowSetImpl() {
        this(OrderedLongSet.EMPTY);
    }

    public WritableRowSetImpl(final OrderedLongSet innerSet) {
        this.innerSet = Objects.requireNonNull(innerSet);
    }

    protected final OrderedLongSet getInnerSet() {
        return innerSet;
    }

    @Override
    public final WritableRowSet copy() {
        return new WritableRowSetImpl(innerSet.ixCowRef());
    }

    @Override
    public TrackingWritableRowSet toTracking() {
        final TrackingWritableRowSet result = new TrackingWritableRowSetImpl(innerSet);
        innerSet = null; // Force NPE on use after tracking
        closeRowSequenceAsChunkImpl();
        return result;
    }

    @OverridingMethodsMustInvokeSuper
    @Override
    public void close() {
        innerSet.ixRelease();
        innerSet = null; // Force NPE on use after close
        closeRowSequenceAsChunkImpl();
    }

    @VisibleForTesting
    final int refCount() {
        return innerSet.ixRefCount();
    }

    protected void preMutationHook() {}

    protected void postMutationHook() {}

    private void assign(final OrderedLongSet maybeNewImpl) {
        invalidateRowSequenceAsChunkImpl();
        if (maybeNewImpl == innerSet) {
            return;
        }
        innerSet.ixRelease();
        innerSet = maybeNewImpl;
    }

    @Override
    public final void insert(final long key) {
        preMutationHook();
        assign(innerSet.ixInsert(key));
        postMutationHook();
    }

    @Override
    public final void insertRange(final long startKey, final long endKey) {
        preMutationHook();
        assign(innerSet.ixInsertRange(startKey, endKey));
        postMutationHook();
    }

    @Override
    public final void insert(final LongChunk<OrderedRowKeys> keys, final int offset, final int length) {
        Assert.leq(offset + length, "offset + length", keys.size(), "keys.size()");
        preMutationHook();
        assign(innerSet.ixInsert(keys, offset, length));
        postMutationHook();
    }

    @Override
    public final void insert(final RowSet added) {
        preMutationHook();
        assign(innerSet.ixInsert(getInnerSet(added)));
        postMutationHook();
    }

    @Override
    public final void remove(final long key) {
        preMutationHook();
        assign(innerSet.ixRemove(key));
        postMutationHook();
    }

    @Override
    public final void removeRange(final long start, final long end) {
        preMutationHook();
        assign(innerSet.ixRemoveRange(start, end));
        postMutationHook();
    }

    @Override
    public final void remove(final LongChunk<OrderedRowKeys> keys, final int offset, final int length) {
        Assert.leq(offset + length, "offset + length", keys.size(), "keys.size()");
        preMutationHook();
        assign(innerSet.ixRemove(keys, offset, length));
        postMutationHook();
    }

    @Override
    public final void remove(final RowSet removed) {
        preMutationHook();
        assign(innerSet.ixRemove(getInnerSet(removed)));
        postMutationHook();
    }

    @Override
    public final void update(final RowSet added, final RowSet removed) {
        preMutationHook();
        assign(innerSet.ixUpdate(getInnerSet(added), getInnerSet(removed)));
        postMutationHook();
    }

    @Override
    public final void retain(final RowSet rowSetToIntersect) {
        preMutationHook();
        assign(innerSet.ixRetain(getInnerSet(rowSetToIntersect)));
        postMutationHook();
    }

    @Override
    public final void retainRange(final long startRowKey, final long endRowKey) {
        preMutationHook();
        assign(innerSet.ixRetainRange(startRowKey, endRowKey));
        postMutationHook();
    }

    @Override
    public final void clear() {
        preMutationHook();
        assign(OrderedLongSet.EMPTY);
        postMutationHook();
    }

    @Override
    public final void shiftInPlace(final long shiftAmount) {
        preMutationHook();
        assign(innerSet.ixShiftInPlace(shiftAmount));
        postMutationHook();
    }

    @Override
    public final void insertWithShift(final long shiftAmount, final RowSet other) {
        preMutationHook();
        assign(innerSet.ixInsertWithShift(shiftAmount, getInnerSet(other)));
        postMutationHook();
    }

    @Override
    public final void compact() {
        // Compact does not change the row keys represented by this RowSet, and thus does not require a call to
        // preMutationHook() or postMutationHook().
        assign(innerSet.ixCompact());
    }

    @Override
    public final long size() {
        return innerSet.ixCardinality();
    }

    @Override
    public final boolean isEmpty() {
        return innerSet.ixIsEmpty();
    }

    @Override
    public final long firstRowKey() {
        return innerSet.ixFirstKey();
    }

    @Override
    public final long lastRowKey() {
        return innerSet.ixLastKey();
    }

    @Override
    public final long rangesCountUpperBound() {
        return innerSet.ixRangesCountUpperBound();
    }

    @Override
    public final RowSequence.Iterator getRowSequenceIterator() {
        return innerSet.ixGetRowSequenceIterator();
    }

    @Override
    public final RowSequence getRowSequenceByPosition(final long startPositionInclusive, final long length) {
        return innerSet.ixGetRowSequenceByPosition(startPositionInclusive, length);
    }

    @Override
    public final RowSequence getRowSequenceByKeyRange(final long startRowKeyInclusive, final long endRowKeyInclusive) {
        return innerSet.ixGetRowSequenceByKeyRange(startRowKeyInclusive, endRowKeyInclusive);
    }

    @Override
    public final RowSet asRowSet() {
        return copy();
    }

    @Override
    public final WritableRowSet invert(final RowSet keys, final long maximumPosition) {
        return new WritableRowSetImpl(innerSet.ixInvertOnNew(getInnerSet(keys), maximumPosition));
    }

    @Override
    public final TLongArrayList[] findMissing(final RowSet keys) {
        return RowSetUtils.findMissing(this, keys);
    }

    @NotNull
    @Override
    public final WritableRowSet intersect(@NotNull final RowSet range) {
        return new WritableRowSetImpl(innerSet.ixIntersectOnNew(getInnerSet(range)));
    }

    @Override
    public final boolean overlaps(@NotNull final RowSet range) {
        return innerSet.ixOverlaps(getInnerSet(range));
    }

    @Override
    public final boolean overlapsRange(final long start, final long end) {
        return innerSet.ixOverlapsRange(start, end);
    }

    @Override
    public final boolean subsetOf(@NotNull final RowSet other) {
        return innerSet.ixSubsetOf(getInnerSet(other));
    }

    @Override
    public final WritableRowSet minus(final RowSet indexToRemove) {
        if (indexToRemove == this) {
            return RowSetFactory.empty();
        }
        return new WritableRowSetImpl(innerSet.ixMinusOnNew(getInnerSet(indexToRemove)));
    }

    @Override
    public final WritableRowSet union(final RowSet indexToAdd) {
        if (indexToAdd == this) {
            return copy();
        }
        return new WritableRowSetImpl(innerSet.ixUnionOnNew(getInnerSet(indexToAdd)));
    }

    @Override
    public final WritableRowSet shift(final long shiftAmount) {
        return new WritableRowSetImpl(innerSet.ixShiftOnNew(shiftAmount));
    }

    @Override
    public final void validate(final String failMsg) {
        innerSet.ixValidate(failMsg);
        long totalSize = 0;
        final RangeIterator it = rangeIterator();
        long lastEnd = Long.MIN_VALUE;
        final String m = failMsg == null ? "" : failMsg + " ";
        while (it.hasNext()) {
            it.next();
            final long start = it.currentRangeStart();
            final long end = it.currentRangeEnd();
            Assert.assertion(start >= 0, m + "start >= 0", start, "start", this, "rowSet");
            Assert.assertion(end >= start, m + "end >= start", start, "start", end, "end", this, "rowSet");
            Assert.assertion(start > lastEnd, m + "start > lastEnd", start, "start", lastEnd, "lastEnd", this,
                    "rowSet");
            Assert.assertion(start > lastEnd + 1, m + "start > lastEnd + 1", start, "start", lastEnd, "lastEnd", this,
                    "rowSet");
            lastEnd = end;

            totalSize += ((end - start) + 1);
        }

        Assert.eq(totalSize, m + "totalSize", size(), "size()");
    }

    @Override
    public final boolean forEachRowKey(final LongAbortableConsumer lc) {
        return innerSet.ixForEachLong(lc);
    }

    @Override
    public final boolean forEachRowKeyRange(final LongRangeAbortableConsumer larc) {
        return innerSet.ixForEachLongRange(larc);
    }

    @Override
    public final WritableRowSet subSetByPositionRange(final long startPos, final long endPos) {
        return new WritableRowSetImpl(innerSet.ixSubindexByPosOnNew(startPos, endPos));
    }

    @Override
    public final WritableRowSet subSetByKeyRange(final long startKey, final long endKey) {
        return new WritableRowSetImpl(innerSet.ixSubindexByKeyOnNew(startKey, endKey));
    }

    @Override
    public final WritableRowSet subSetForPositions(RowSequence posRowSequence, boolean reversed) {
        if (reversed) {
            return subSetForReversePositions(posRowSequence);
        }
        return subSetForPositions(posRowSequence);
    }

    @Override
    public final WritableRowSet subSetForPositions(RowSequence positions) {
        if (positions.isEmpty()) {
            return RowSetFactory.empty();
        }
        if (positions.isContiguous()) {
            return subSetByPositionRange(positions.firstRowKey(), positions.lastRowKey() + 1);
        }
        final MutableLong currentOffset = new MutableLong();
        final RowSequence.Iterator iter = getRowSequenceIterator();
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        positions.forEachRowKeyRange((start, end) -> {
            if (currentOffset.longValue() < start) {
                // skip items until the beginning of this range
                iter.getNextRowSequenceWithLength(start - currentOffset.longValue());
                currentOffset.setValue(start);
            }
            if (!iter.hasMore()) {
                return false;
            }
            iter.getNextRowSequenceWithLength(end + 1 - currentOffset.longValue())
                    .forAllRowKeyRanges(builder::appendRange);
            currentOffset.setValue(end + 1);
            return iter.hasMore();
        });
        return builder.build();
    }

    @Override
    public final WritableRowSet subSetForReversePositions(RowSequence positions) {
        if (positions.isEmpty()) {
            return RowSetFactory.empty();
        }

        final long lastRowPosition = size() - 1;
        if (positions.size() == positions.lastRowKey() - positions.firstRowKey() + 1) {
            // We have a single range in the input sequence
            final long forwardEnd = lastRowPosition - positions.firstRowKey();
            if (forwardEnd < 0) {
                // The single range does not overlap with the available positions at all
                return RowSetFactory.empty();
            }
            // Clamp the single range end to 0
            final long forwardStart = Math.max(lastRowPosition - positions.lastRowKey(), 0);
            try (final RowSequence forwardPositions = RowSequenceFactory.forRange(forwardStart, forwardEnd)) {
                return subSetForPositions(forwardPositions);
            }
        }

        // We have some non-trivial input sequence
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        positions.forEachRowKeyRange((start, end) -> {
            final long forwardEnd = lastRowPosition - start;
            if (forwardEnd < 0) {
                // This range does not overlap with the available positions at all, and thus neither can subsequent
                // ranges that are offset further from the lastRowPosition.
                return false;
            }
            // Clamp the range end to 0
            final long forwardStart = Math.max(lastRowPosition - end, 0);
            builder.addRange(forwardStart, forwardEnd);

            // Continue iff subsequent ranges may overlap the available positions
            return forwardStart != 0;
        });
        try (final RowSequence forwardPositions = builder.build()) {
            return subSetForPositions(forwardPositions);
        }
    }

    @Override
    public final long get(final long rowPosition) {
        return innerSet.ixGet(rowPosition);
    }

    @Override
    public final void getKeysForPositions(PrimitiveIterator.OfLong positions, LongConsumer outputKeys) {
        innerSet.ixGetKeysForPositions(positions, outputKeys);
    }

    @Override
    public final long find(final long key) {
        return innerSet.ixFind(key);
    }

    @NotNull
    @Override
    public final RowSet.Iterator iterator() {
        return innerSet.ixIterator();
    }

    @Override
    public final SearchIterator searchIterator() {
        return innerSet.ixSearchIterator();
    }

    @Override
    public final SearchIterator reverseIterator() {
        return innerSet.ixReverseIterator();
    }

    @Override
    public final RangeIterator rangeIterator() {
        return innerSet.ixRangeIterator();
    }

    @Override
    public final long getAverageRunLengthEstimate() {
        return innerSet.ixGetAverageRunLengthEstimate();
    }

    @Override
    public final boolean containsRange(final long start, final long end) {
        return innerSet.ixContainsRange(start, end);
    }

    @Override
    public final void fillRowKeyChunk(final WritableLongChunk<? super OrderedRowKeys> chunkToFill) {
        RowSetUtils.fillKeyIndicesChunk(this, chunkToFill);
    }

    @Override
    public final void fillRowKeyRangesChunk(final WritableLongChunk<OrderedRowKeyRanges> chunkToFill) {
        RowSetUtils.fillKeyRangesChunk(this, chunkToFill);
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return RowSetUtils.append(logOutput, rangeIterator());
    }

    @Override
    public String toString() {
        return RowSetUtils.toString(this, 200);
    }

    public String toString(final int maxRanges) {
        return RowSetUtils.toString(this, maxRanges);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public final boolean equals(final Object obj) {
        return RowSetUtils.equals(this, obj);
    }

    @Override
    public final void writeExternal(@NotNull final ObjectOutput out) throws IOException {
        ExternalizableRowSetUtils.writeExternalCompressedDeltas(out, this);
    }

    @Override
    public void readExternal(@NotNull final ObjectInput in) throws IOException {
        try (final RowSet readRowSet = ExternalizableRowSetUtils.readExternalCompressedDelta(in)) {
            assign(getInnerSet(readRowSet).ixCowRef());
        }
    }

    /**
     * Debugging tool to serialize the inner set implementation.
     *
     * @param out The destination
     */
    @SuppressWarnings("unused")
    public void writeImpl(ObjectOutput out) throws IOException {
        out.writeObject(innerSet);
    }

    public static void addToBuilderFromImpl(final OrderedLongSet.BuilderRandom builder,
            final WritableRowSetImpl rowSet) {
        if (rowSet.innerSet instanceof SingleRange) {
            builder.add((SingleRange) rowSet.innerSet);
            return;
        }
        if (rowSet.innerSet instanceof SortedRanges) {
            builder.add((SortedRanges) rowSet.innerSet, true);
            return;
        }
        final RspBitmap idxImpl = (RspBitmap) rowSet.innerSet;
        builder.add(idxImpl, true);
    }

    protected static OrderedLongSet getInnerSet(final RowSet rowSet) {
        if (rowSet instanceof WritableRowSetImpl) {
            return ((WritableRowSetImpl) rowSet).getInnerSet();
        }
        throw new UnsupportedOperationException("Unexpected RowSet type " + rowSet.getClass());
    }
}
