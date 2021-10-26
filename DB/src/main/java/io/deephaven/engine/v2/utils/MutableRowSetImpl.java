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
import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;

public class MutableRowSetImpl extends RowSequenceAsChunkImpl implements MutableRowSet, Externalizable {

    private static final long serialVersionUID = 1L;

    private TreeIndexImpl impl;

    @SuppressWarnings("WeakerAccess") // Mandatory for Externalizable
    public MutableRowSetImpl() {
        this(TreeIndexImpl.EMPTY);
    }

    public MutableRowSetImpl(final TreeIndexImpl impl) {
        this.impl = impl;
    }

    protected final TreeIndexImpl getImpl() {
        return impl;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public final MutableRowSet clone() {
        return new MutableRowSetImpl(impl.ixCowRef());
    }

    @Override
    public TrackingMutableRowSet tracking() {
        impl = null; // Force NPE on use after tracking
        closeRowSequenceAsChunkImpl();
        return new TrackingMutableRowSetImpl(impl);
    }

    @OverridingMethodsMustInvokeSuper
    @Override
    public void close() {
        impl.ixRelease();
        impl = null; // Force NPE on use after close
        closeRowSequenceAsChunkImpl();
    }

    @VisibleForTesting
    final int refCount() {
        return impl.ixRefCount();
    }

    protected void preMutationHook() {}

    protected void postMutationHook() {}

    private void assign(final TreeIndexImpl maybeNewImpl) {
        invalidateRowSequenceAsChunkImpl();
        if (maybeNewImpl == impl) {
            return;
        }
        impl.ixRelease();
        impl = maybeNewImpl;
    }

    @Override
    public final void insert(final long key) {
        preMutationHook();
        assign(impl.ixInsert(key));
        postMutationHook();
    }

    @Override
    public final void insertRange(final long startKey, final long endKey) {
        preMutationHook();
        assign(impl.ixInsertRange(startKey, endKey));
        postMutationHook();
    }

    @Override
    public final void insert(final LongChunk<OrderedRowKeys> keys, final int offset, final int length) {
        Assert.leq(offset + length, "offset + length", keys.size(), "keys.size()");
        preMutationHook();
        assign(impl.ixInsert(keys, offset, length));
        postMutationHook();
    }

    @Override
    public final void insert(final RowSet added) {
        preMutationHook();
        assign(impl.ixInsert(getImpl(added)));
        postMutationHook();
    }

    @Override
    public final void remove(final long key) {
        preMutationHook();
        assign(impl.ixRemove(key));
        postMutationHook();
    }

    @Override
    public final void removeRange(final long start, final long end) {
        preMutationHook();
        assign(impl.ixRemoveRange(start, end));
        postMutationHook();
    }

    @Override
    public final void remove(final LongChunk<OrderedRowKeys> keys, final int offset, final int length) {
        Assert.leq(offset + length, "offset + length", keys.size(), "keys.size()");
        preMutationHook();
        assign(impl.ixRemove(keys, offset, length));
        postMutationHook();
    }

    @Override
    public final void remove(final RowSet removed) {
        preMutationHook();
        assign(impl.ixRemove(getImpl(removed)));
        postMutationHook();
    }

    @Override
    public final void update(final RowSet added, final RowSet removed) {
        preMutationHook();
        assign(impl.ixUpdate(getImpl(added), getImpl(removed)));
        postMutationHook();
    }

    @Override
    public final void retain(final RowSet rowSetToIntersect) {
        preMutationHook();
        assign(impl.ixRetain(getImpl(rowSetToIntersect)));
        postMutationHook();
    }

    @Override
    public final void retainRange(final long startRowKey, final long endRowKey) {
        preMutationHook();
        assign(impl.ixRetainRange(startRowKey, endRowKey));
        postMutationHook();
    }

    @Override
    public final void clear() {
        preMutationHook();
        assign(TreeIndexImpl.EMPTY);
        postMutationHook();
    }

    @Override
    public final void shiftInPlace(final long shiftAmount) {
        preMutationHook();
        assign(impl.ixShiftInPlace(shiftAmount));
        postMutationHook();
    }

    @Override
    public final void insertWithShift(final long shiftAmount, final RowSet other) {
        preMutationHook();
        assign(impl.ixInsertWithShift(shiftAmount, getImpl(other)));
        postMutationHook();
    }

    @Override
    public final void compact() {
        // Compact does not change the row keys represented by this RowSet, and thus does not require a call to
        // preMutationHook() or postMutationHook().
        assign(impl.ixCompact());
    }

    @Override
    public final long size() {
        return impl.ixCardinality();
    }

    @Override
    public final boolean isEmpty() {
        return impl.ixIsEmpty();
    }

    @Override
    public final long firstRowKey() {
        return impl.ixFirstKey();
    }

    @Override
    public final long lastRowKey() {
        return impl.ixLastKey();
    }

    @Override
    public final long rangesCountUpperBound() {
        return impl.ixRangesCountUpperBound();
    }

    @Override
    public final RowSequence.Iterator getRowSequenceIterator() {
        return impl.ixGetRowSequenceIterator();
    }

    @Override
    public final RowSequence getRowSequenceByPosition(final long startPositionInclusive, final long length) {
        return impl.ixGetRowSequenceByPosition(startPositionInclusive, length);
    }

    @Override
    public final RowSequence getRowSequenceByKeyRange(final long startRowKeyInclusive, final long endRowKeyInclusive) {
        return impl.ixGetRowSequenceByKeyRange(startRowKeyInclusive, endRowKeyInclusive);
    }

    @Override
    public final RowSet asRowSet() {
        return this;
    }

    @Override
    public final RowSet invert(final RowSet keys, final long maximumPosition) {
        return new MutableRowSetImpl(impl.ixInvertOnNew(getImpl(keys), maximumPosition));
    }

    @Override
    public final TLongArrayList[] findMissing(final RowSet keys) {
        return RowSetUtilities.findMissing(this, keys);
    }

    @NotNull
    @Override
    public final MutableRowSet intersect(@NotNull final RowSet range) {
        return new MutableRowSetImpl(impl.ixIntersectOnNew(getImpl(range)));
    }

    @Override
    public final boolean overlaps(@NotNull final RowSet range) {
        return impl.ixOverlaps(getImpl(range));
    }

    @Override
    public final boolean overlapsRange(final long start, final long end) {
        return impl.ixOverlapsRange(start, end);
    }

    @Override
    public final boolean subsetOf(@NotNull final RowSet other) {
        return impl.ixSubsetOf(getImpl(other));
    }

    @Override
    public final MutableRowSet minus(final RowSet indexToRemove) {
        if (indexToRemove == this) {
            return RowSetFactoryImpl.INSTANCE.getEmptyRowSet();
        }
        return new MutableRowSetImpl(impl.ixMinusOnNew(getImpl(indexToRemove)));
    }

    @Override
    public final MutableRowSet union(final RowSet indexToAdd) {
        if (indexToAdd == this) {
            return clone();
        }
        return new MutableRowSetImpl(impl.ixUnionOnNew(getImpl(indexToAdd)));
    }

    @Override
    public final RowSet shift(final long shiftAmount) {
        return new MutableRowSetImpl(impl.ixShiftOnNew(shiftAmount));
    }

    @Override
    public final void validate(final String failMsg) {
        impl.ixValidate(failMsg);
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
    public final boolean forEachLong(final LongAbortableConsumer lc) {
        return impl.ixForEachLong(lc);
    }

    @Override
    public final boolean forEachLongRange(final LongRangeAbortableConsumer larc) {
        return impl.ixForEachLongRange(larc);
    }

    @Override
    public final MutableRowSet subSetByPositionRange(final long startPos, final long endPos) {
        return new MutableRowSetImpl(impl.ixSubindexByPosOnNew(startPos, endPos));
    }

    @Override
    public final MutableRowSet subSetByKeyRange(final long startKey, final long endKey) {
        return new MutableRowSetImpl(impl.ixSubindexByKeyOnNew(startKey, endKey));
    }

    @Override
    public final long get(final long pos) {
        return impl.ixGet(pos);
    }

    @Override
    public final void getKeysForPositions(PrimitiveIterator.OfLong positions, LongConsumer outputKeys) {
        impl.ixGetKeysForPositions(positions, outputKeys);
    }

    @Override
    public final long find(final long key) {
        return impl.ixFind(key);
    }

    @NotNull
    @Override
    public final RowSet.Iterator iterator() {
        return impl.ixIterator();
    }

    @Override
    public final SearchIterator searchIterator() {
        return impl.ixSearchIterator();
    }

    @Override
    public final SearchIterator reverseIterator() {
        return impl.ixReverseIterator();
    }

    @Override
    public final RangeIterator rangeIterator() {
        return impl.ixRangeIterator();
    }

    @Override
    public final long getAverageRunLengthEstimate() {
        return impl.ixGetAverageRunLengthEstimate();
    }

    @Override
    public final boolean containsRange(final long start, final long end) {
        return impl.ixContainsRange(start, end);
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
            assign(getImpl(readRowSet).ixCowRef());
        }
    }

    /**
     * Debugging tool to serialize the inner set implementation.
     *
     * @param out The destination
     */
    @SuppressWarnings("unused")
    public void writeImpl(ObjectOutput out) throws IOException {
        out.writeObject(impl);
    }

    public static void addToBuilderFromImpl(final TreeIndexImpl.BuilderRandom builder, final MutableRowSetImpl rowSet) {
        if (rowSet.impl instanceof SingleRange) {
            builder.add((SingleRange) rowSet.impl);
            return;
        }
        if (rowSet.impl instanceof SortedRanges) {
            builder.add((SortedRanges) rowSet.impl, true);
            return;
        }
        final RspBitmap idxImpl = (RspBitmap) rowSet.impl;
        builder.add(idxImpl, true);
    }

    protected static TreeIndexImpl getImpl(final RowSet rowSet) {
        if (rowSet instanceof MutableRowSetImpl) {
            return ((MutableRowSetImpl) rowSet).getImpl();
        }
        throw new UnsupportedOperationException("Unexpected RowSet type " + rowSet.getClass());
    }
}
