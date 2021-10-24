package io.deephaven.engine.v2.utils;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.structures.rowsequence.RowSequenceAsChunkImpl;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.chunk.Attributes;
import io.deephaven.engine.v2.sources.chunk.Attributes.OrderedRowKeys;
import io.deephaven.engine.v2.sources.chunk.LongChunk;
import io.deephaven.engine.v2.sources.chunk.WritableLongChunk;
import io.deephaven.engine.v2.tuples.TupleSource;
import gnu.trove.list.array.TLongArrayList;
import org.jetbrains.annotations.NotNull;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.LongConsumer;

public class MutableRowSetImpl extends RowSequenceAsChunkImpl
        implements ImplementedByTreeIndexImpl, MutableRowSet, Externalizable {

    private static final long serialVersionUID = 1L;

    private TreeIndexImpl impl;

    @SuppressWarnings("WeakerAccess") // Mandatory for Externalizable
    public MutableRowSetImpl() {
        this(TreeIndexImpl.EMPTY);
    }

    public MutableRowSetImpl(final TreeIndexImpl impl) {
        this.impl = impl;
    }

    @Override
    final public TreeIndexImpl getImpl() {
        return impl;
    }

    @Override
    public TrackingMutableRowSet tracking() {
        impl = null; // Force NPE on use after tracking
        closeRowSequenceAsChunkImpl();
        return new TrackingMutableRowSetImpl(impl);
    }

    @Override
    public void close() {
        impl.ixRelease();
        impl = null; // Force NPE on use after close
        closeRowSequenceAsChunkImpl();
    }

    @Override
    public int refCount() {
        return impl.ixRefCount();
    }

    @Override
    public void insert(final long key) {
        assign(impl.ixInsert(key));
    }

    @Override
    public void insertRange(final long startKey, final long endKey) {
        assign(impl.ixInsertRange(startKey, endKey));
    }

    @Override
    public void insert(final LongChunk<OrderedRowKeys> keys, final int offset, final int length) {
        Assert.leq(offset + length, "offset + length", keys.size(), "keys.size()");
        assign(impl.ixInsert(keys, offset, length));
    }

    @Override
    public void insert(final RowSet added) {
        assign(impl.ixInsert(getImpl(added)));
    }

    @Override
    public void remove(final long key) {
        assign(impl.ixRemove(key));
    }

    @Override
    public void removeRange(final long start, final long end) {
        assign(impl.ixRemoveRange(start, end));
    }

    @Override
    public void remove(final LongChunk<OrderedRowKeys> keys, final int offset, final int length) {
        Assert.leq(offset + length, "offset + length", keys.size(), "keys.size()");
        assign(impl.ixRemove(keys, offset, length));
    }

    @Override
    public void remove(final RowSet removed) {
        assign(impl.ixRemove(getImpl(removed)));
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public MutableRowSetImpl clone() {
        return new MutableRowSetImpl(impl.ixCowRef());
    }

    @Override
    public void retain(final RowSet rowSetToIntersect) {
        assign(impl.ixRetain(getImpl(rowSetToIntersect)));
    }

    @Override
    public void retainRange(final long startRowKey, final long endRowKey) {
        assign(impl.ixRetainRange(startRowKey, endRowKey));
    }

    @Override
    public void update(final RowSet added, final RowSet removed) {
        assign(impl.ixUpdate(getImpl(added), getImpl(removed)));
    }

    @Override
    public long lastRowKey() {
        return impl.ixLastKey();
    }

    @Override
    public long rangesCountUpperBound() {
        return impl.ixRangesCountUpperBound();
    }

    @Override
    public RowSequence.Iterator getRowSequenceIterator() {
        return impl.ixGetRowSequenceIterator();
    }

    @Override
    public RowSequence getRowSequenceByPosition(final long startPositionInclusive, final long length) {
        return impl.ixGetRowSequenceByPosition(startPositionInclusive, length);
    }

    @Override
    public RowSequence getRowSequenceByKeyRange(final long startRowKeyInclusive, final long endRowKeyInclusive) {
        return impl.ixGetRowSequenceByKeyRange(startRowKeyInclusive, endRowKeyInclusive);
    }

    @Override
    public TrackingMutableRowSet asIndex() {
        return new TrackingMutableRowSetImpl(impl);
    }


    @Override
    public long firstRowKey() {
        return impl.ixFirstKey();
    }

    @Override
    public MutableRowSet invert(final RowSet keys) {
        return invert(keys, Long.MAX_VALUE);

    }

    @Override
    public MutableRowSet invert(final RowSet keys, final long maximumPosition) {
        return new MutableRowSetImpl(impl.ixInvertOnNew(getImpl(keys), maximumPosition));
    }

    @Override
    public TLongArrayList[] findMissing(final RowSet keys) {
        return IndexUtilities.findMissing(this, keys);
    }

    @NotNull
    @Override
    public MutableRowSet intersect(@NotNull final RowSet range) {
        return new MutableRowSetImpl(impl.ixIntersectOnNew(getImpl(range)));
    }

    @Override
    public boolean overlapsRange(final long start, final long end) {
        return impl.ixOverlapsRange(start, end);
    }

    @Override
    public boolean subsetOf(@NotNull final RowSet other) {
        return impl.ixSubsetOf(getImpl(other));
    }

    @Override
    public MutableRowSet minus(final RowSet indexToRemove) {
        return new MutableRowSetImpl(impl.ixMinusOnNew(getImpl(indexToRemove)));
    }

    @Override
    public MutableRowSet union(final RowSet indexToAdd) {
        return new MutableRowSetImpl(impl.ixUnionOnNew(getImpl(indexToAdd)));
    }

    @Override
    public void clear() {
        assign(TreeIndexImpl.EMPTY);
    }

    @Override
    public MutableRowSet shift(final long shiftAmount) {
        return new MutableRowSetImpl(impl.ixShiftOnNew(shiftAmount));
    }

    @Override
    public void shiftInPlace(final long shiftAmount) {
        assign(impl.ixShiftInPlace(shiftAmount));
    }

    @Override
    public void insertWithShift(final long shiftAmount, final RowSet other) {
        assign(impl.ixInsertWithShift(shiftAmount, getImpl(other)));
    }

    @Override
    public void compact() {
        assign(impl.ixCompact());
    }

    @Override
    public void validate(final String failMsg) {
        impl.ixValidate(failMsg);
    }

    @Override
    public boolean forEachLong(final LongAbortableConsumer lc) {
        return impl.ixForEachLong(lc);
    }

    @Override
    public boolean forEachLongRange(final LongRangeAbortableConsumer larc) {
        return impl.ixForEachLongRange(larc);
    }

    @Override
    public MutableRowSet subSetByPositionRange(final long startPos, final long endPos) {
        return new MutableRowSetImpl(impl.ixSubindexByPosOnNew(startPos, endPos));
    }

    @Override
    public MutableRowSet subSetByKeyRange(final long startKey, final long endKey) {
        return new MutableRowSetImpl(impl.ixSubindexByKeyOnNew(startKey, endKey));
    }

    @Override
    public long get(final long pos) {
        return impl.ixGet(pos);
    }

    @Override
    public void getKeysForPositions(PrimitiveIterator.OfLong positions, LongConsumer outputKeys) {
        impl.ixGetKeysForPositions(positions, outputKeys);
    }

    @Override
    public long find(final long key) {
        return impl.ixFind(key);
    }

    @NotNull
    @Override
    public TrackingMutableRowSet.Iterator iterator() {
        return impl.ixIterator();
    }

    @Override
    public SearchIterator searchIterator() {
        return impl.ixSearchIterator();
    }

    @Override
    public SearchIterator reverseIterator() {
        return impl.ixReverseIterator();
    }

    @Override
    public RangeIterator rangeIterator() {
        return impl.ixRangeIterator();
    }

    @Override
    public long size() {
        return impl.ixCardinality();
    }

    @Override
    public long getAverageRunLengthEstimate() {
        return impl.ixGetAverageRunLengthEstimate();
    }

    @Override
    public final boolean isFlat() {
        return empty() || (lastRowKey() == size() - 1);
    }

    @Override
    public boolean empty() {
        return impl.ixIsEmpty();
    }

    @Override
    public boolean containsRange(final long start, final long end) {
        return impl.ixContainsRange(start, end);
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return IndexUtilities.append(logOutput, rangeIterator());
    }

    // Through this the contract for RowSet could be bypassed; it is not the intention.
    private static TreeIndexImpl getImpl(final RowSet index) {
        if (index instanceof ImplementedByTreeIndexImpl) {
            return ((ImplementedByTreeIndexImpl) index).getImpl();
        }
        throw new UnsupportedOperationException();
    }

    private void assign(final TreeIndexImpl maybeNewImpl) {
        invalidateRowSequenceAsChunkImpl();
        if (maybeNewImpl == impl) {
            return;
        }
        impl.ixRelease();
        impl = maybeNewImpl;
    }

    @Override
    public void fillRowKeyChunk(final WritableLongChunk<? extends Attributes.RowKeys> chunkToFill) {
        IndexUtilities.fillKeyIndicesChunk(this, chunkToFill);
    }

    @Override
    public void fillRowKeyRangesChunk(final WritableLongChunk<Attributes.OrderedRowKeyRanges> chunkToFill) {
        IndexUtilities.fillKeyRangesChunk(this, chunkToFill);
    }

    @Override
    public String toString() {
        return IndexUtilities.toString(this, 200);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(final Object obj) {
        return IndexUtilities.equals(this, obj);
    }

    @Override
    public final void writeExternal(@NotNull final ObjectOutput out) throws IOException {
        ExternalizableIndexUtils.writeExternalCompressedDeltas(out, this);
    }

    @Override
    public final void readExternal(@NotNull final ObjectInput in) throws IOException {
        try (final MutableRowSet readRowSet = ExternalizableIndexUtils.readExternalCompressedDelta(in)) {
            insert(readRowSet);
        }
    }
}
