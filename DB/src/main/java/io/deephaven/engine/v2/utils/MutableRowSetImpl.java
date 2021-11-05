package io.deephaven.engine.v2.utils;

import gnu.trove.list.array.TLongArrayList;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.structures.rowsequence.RowSequenceAsChunkImpl;
import io.deephaven.engine.v2.sources.chunk.Attributes.OrderedRowKeyRanges;
import io.deephaven.engine.v2.sources.chunk.Attributes.OrderedRowKeys;
import io.deephaven.engine.v2.sources.chunk.Attributes.RowKeys;
import io.deephaven.engine.v2.sources.chunk.LongChunk;
import io.deephaven.engine.v2.sources.chunk.WritableLongChunk;
import io.deephaven.engine.v2.utils.rsp.RspBitmap;
import io.deephaven.engine.v2.utils.singlerange.SingleRange;
import io.deephaven.engine.v2.utils.sortedranges.SortedRanges;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;

public class MutableRowSetImpl extends RowSequenceAsChunkImpl implements MutableRowSet, Externalizable {

    private static final long serialVersionUID = 1L;

    private OrderedLongSet innerSet;

    @SuppressWarnings("WeakerAccess") // Mandatory for Externalizable
    public MutableRowSetImpl() {
        this(OrderedLongSet.EMPTY);
    }

    public MutableRowSetImpl(final OrderedLongSet innerSet) {
        this.innerSet = Objects.requireNonNull(innerSet);
    }

    protected final OrderedLongSet getInnerSet() {
        return innerSet;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public final MutableRowSet clone() {
        return new MutableRowSetImpl(innerSet.ixCowRef());
    }

    @Override
    public TrackingMutableRowSet convertToTracking() {
        final TrackingMutableRowSet result = new TrackingMutableRowSetImpl(innerSet);
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
        return clone();
    }

    @Override
    public final MutableRowSet invert(final RowSet keys, final long maximumPosition) {
        return new MutableRowSetImpl(innerSet.ixInvertOnNew(getInnerSet(keys), maximumPosition));
    }

    @Override
    public final TLongArrayList[] findMissing(final RowSet keys) {
        return RowSetUtilities.findMissing(this, keys);
    }

    @NotNull
    @Override
    public final MutableRowSet intersect(@NotNull final RowSet range) {
        return new MutableRowSetImpl(innerSet.ixIntersectOnNew(getInnerSet(range)));
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
    public final MutableRowSet minus(final RowSet indexToRemove) {
        if (indexToRemove == this) {
            return RowSetFactory.empty();
        }
        return new MutableRowSetImpl(innerSet.ixMinusOnNew(getInnerSet(indexToRemove)));
    }

    @Override
    public final MutableRowSet union(final RowSet indexToAdd) {
        if (indexToAdd == this) {
            return clone();
        }
        return new MutableRowSetImpl(innerSet.ixUnionOnNew(getInnerSet(indexToAdd)));
    }

    @Override
    public final MutableRowSet shift(final long shiftAmount) {
        return new MutableRowSetImpl(innerSet.ixShiftOnNew(shiftAmount));
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
    public final MutableRowSet subSetByPositionRange(final long startPos, final long endPos) {
        return new MutableRowSetImpl(innerSet.ixSubindexByPosOnNew(startPos, endPos));
    }

    @Override
    public final MutableRowSet subSetByKeyRange(final long startKey, final long endKey) {
        return new MutableRowSetImpl(innerSet.ixSubindexByKeyOnNew(startKey, endKey));
    }

    @Override
    public final long get(final long pos) {
        return innerSet.ixGet(pos);
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
    public final void fillRowKeyChunk(final WritableLongChunk<? extends RowKeys> chunkToFill) {
        RowSetUtilities.fillKeyIndicesChunk(this, chunkToFill);
    }

    @Override
    public final void fillRowKeyRangesChunk(final WritableLongChunk<OrderedRowKeyRanges> chunkToFill) {
        RowSetUtilities.fillKeyRangesChunk(this, chunkToFill);
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return RowSetUtilities.append(logOutput, rangeIterator());
    }

    @Override
    public String toString() {
        return RowSetUtilities.toString(this, 200);
    }

    public String toString(final int maxRanges) {
        return RowSetUtilities.toString(this, maxRanges);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public final boolean equals(final Object obj) {
        return RowSetUtilities.equals(this, obj);
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

    public static void addToBuilderFromImpl(final OrderedLongSet.BuilderRandom builder, final MutableRowSetImpl rowSet) {
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
        if (rowSet instanceof MutableRowSetImpl) {
            return ((MutableRowSetImpl) rowSet).getInnerSet();
        }
        throw new UnsupportedOperationException("Unexpected RowSet type " + rowSet.getClass());
    }
}
