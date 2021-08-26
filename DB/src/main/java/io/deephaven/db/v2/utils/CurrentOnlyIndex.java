package io.deephaven.db.v2.utils;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.tuples.TupleSource;
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

public class CurrentOnlyIndex extends OrderedKeysAsChunkImpl
        implements ImplementedByTreeIndexImpl, Index, Externalizable {

    private static final long serialVersionUID = 1L;

    private TreeIndexImpl impl;

    @SuppressWarnings("WeakerAccess") // Mandatory for Externalizable
    public CurrentOnlyIndex() {
        this(TreeIndexImpl.EMPTY);
    }

    public CurrentOnlyIndex(final TreeIndexImpl impl) {
        this.impl = impl;
    }

    @Override
    final public TreeIndexImpl getImpl() {
        return impl;
    }

    @Override
    public void close() {
        impl.ixRelease();
        closeOrderedKeysAsChunkImpl();
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
    public void insert(final LongChunk<OrderedKeyIndices> keys, final int offset, final int length) {
        Assert.leq(offset + length, "offset + length", keys.size(), "keys.size()");
        assign(impl.ixInsert(keys, offset, length));
    }

    @Override
    public void insert(final ReadOnlyIndex added) {
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
    public void remove(final LongChunk<OrderedKeyIndices> keys, final int offset, final int length) {
        Assert.leq(offset + length, "offset + length", keys.size(), "keys.size()");
        assign(impl.ixRemove(keys, offset, length));
    }

    @Override
    public void remove(final ReadOnlyIndex removed) {
        assign(impl.ixRemove(getImpl(removed)));
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public CurrentOnlyIndex clone() {
        return new CurrentOnlyIndex(impl.ixCowRef());
    }

    @Override
    public void retain(final ReadOnlyIndex toIntersect) {
        assign(impl.ixRetain(getImpl(toIntersect)));
    }

    @Override
    public void retainRange(final long start, final long end) {
        assign(impl.ixRetainRange(start, end));
    }

    @Override
    public void update(final ReadOnlyIndex added, final ReadOnlyIndex removed) {
        assign(impl.ixUpdate(getImpl(added), getImpl(removed)));
    }

    @Override
    public long lastKey() {
        return impl.ixLastKey();
    }

    @Override
    public long rangesCountUpperBound() {
        return impl.ixRangesCountUpperBound();
    }

    @Override
    public OrderedKeys.Iterator getOrderedKeysIterator() {
        return impl.ixGetOrderedKeysIterator();
    }

    @Override
    public OrderedKeys getOrderedKeysByPosition(final long startPositionInclusive, final long length) {
        return impl.ixGetOrderedKeysByPosition(startPositionInclusive, length);
    }

    @Override
    public OrderedKeys getOrderedKeysByKeyRange(final long startKeyInclusive, final long endKeyInclusive) {
        return impl.ixGetOrderedKeysByKeyRange(startKeyInclusive, endKeyInclusive);
    }

    @Override
    public Index asIndex() {
        return new TreeIndex(impl);
    }


    @Override
    public long firstKey() {
        return impl.ixFirstKey();
    }

    @Override
    public Index invert(final ReadOnlyIndex keys) {
        return invert(keys, Long.MAX_VALUE);

    }

    @Override
    public Index invert(final ReadOnlyIndex keys, final long maximumPosition) {
        return new CurrentOnlyIndex(impl.ixInvertOnNew(getImpl(keys), maximumPosition));
    }

    @Override
    public TLongArrayList[] findMissing(final ReadOnlyIndex keys) {
        return IndexUtilities.findMissing(this, keys);
    }

    @NotNull
    @Override
    public Index intersect(@NotNull final ReadOnlyIndex range) {
        return new CurrentOnlyIndex(impl.ixIntersectOnNew(getImpl(range)));
    }

    @Override
    public boolean overlapsRange(final long start, final long end) {
        return impl.ixOverlapsRange(start, end);
    }

    @Override
    public boolean subsetOf(@NotNull final ReadOnlyIndex other) {
        return impl.ixSubsetOf(getImpl(other));
    }

    @Override
    public Index minus(final ReadOnlyIndex indexToRemove) {
        return new CurrentOnlyIndex(impl.ixMinusOnNew(getImpl(indexToRemove)));
    }

    @Override
    public Index union(final ReadOnlyIndex indexToAdd) {
        return new CurrentOnlyIndex(impl.ixUnionOnNew(getImpl(indexToAdd)));
    }

    @Override
    public void clear() {
        assign(TreeIndexImpl.EMPTY);
    }

    @Override
    public Index shift(final long shiftAmount) {
        return new CurrentOnlyIndex(impl.ixShiftOnNew(shiftAmount));
    }

    @Override
    public void shiftInPlace(final long shiftAmount) {
        assign(impl.ixShiftInPlace(shiftAmount));
    }

    @Override
    public void insertWithShift(final long shiftAmount, final ReadOnlyIndex other) {
        assign(impl.ixInsertWithShift(shiftAmount, getImpl(other)));
    }

    @Override
    public void compact() {
        assign(impl.ixCompact());
    }

    @Override
    public Map<Object, Index> getGrouping(final TupleSource tupleSource) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Object, Index> getPrevGrouping(final TupleSource tupleSource) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copyImmutableGroupings(TupleSource source, TupleSource dest) {}

    @Override
    public Map<Object, Index> getGroupingForKeySet(final Set<Object> keys, final TupleSource tupleSource) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Index getSubIndexForKeySet(final Set<Object> keySet, final TupleSource tupleSource) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasGrouping(final ColumnSource... keyColumns) {
        return false;
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
    public Index subindexByPos(final long startPos, final long endPos) {
        return new CurrentOnlyIndex(impl.ixSubindexByPosOnNew(startPos, endPos));
    }

    @Override
    public Index subindexByKey(final long startKey, final long endKey) {
        return new CurrentOnlyIndex(impl.ixSubindexByKeyOnNew(startKey, endKey));
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
    public long getPrev(final long pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sizePrev() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Index getPrevIndex() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long firstKeyPrev() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long lastKeyPrev() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void initializePreviousValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long find(final long key) {
        return impl.ixFind(key);
    }

    @Override
    public long findPrev(final long key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSorted() {
        return true;
    }

    @NotNull
    @Override
    public Index.Iterator iterator() {
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
        return empty() || (lastKey() == size() - 1);
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

    // Through this the contract for ReadOnlyIndex could be bypassed; it is not the intention.
    private static TreeIndexImpl getImpl(final ReadOnlyIndex index) {
        if (index instanceof ImplementedByTreeIndexImpl) {
            return ((ImplementedByTreeIndexImpl) index).getImpl();
        }
        throw new UnsupportedOperationException();
    }

    private void assign(final TreeIndexImpl maybeNewImpl) {
        invalidateOrderedKeysAsChunkImpl();
        if (maybeNewImpl == impl) {
            return;
        }
        impl.ixRelease();
        impl = maybeNewImpl;
    }

    @Override
    public void fillKeyIndicesChunk(final WritableLongChunk<? extends Attributes.KeyIndices> chunkToFill) {
        IndexUtilities.fillKeyIndicesChunk(this, chunkToFill);
    }

    @Override
    public void fillKeyRangesChunk(final WritableLongChunk<OrderedKeyRanges> chunkToFill) {
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
        try (final Index readIndex = ExternalizableIndexUtils.readExternalCompressedDelta(in)) {
            insert(readIndex);
        }
    }
}
