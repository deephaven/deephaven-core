/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.utils;

import java.io.*;
import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.v2.sources.chunk.Attributes;
import io.deephaven.io.logger.Logger;
import static io.deephaven.engine.v2.sources.chunk.Attributes.RowKeys;
import static io.deephaven.engine.v2.sources.chunk.Attributes.OrderedRowKeys;
import static io.deephaven.engine.v2.sources.chunk.Attributes.OrderedRowKeyRanges;

import io.deephaven.engine.v2.sources.chunk.LongChunk;
import io.deephaven.engine.v2.sources.chunk.WritableLongChunk;
import io.deephaven.engine.v2.sources.LogicalClock;
import io.deephaven.engine.v2.utils.rsp.RspBitmap;
import io.deephaven.engine.v2.utils.sortedranges.SortedRanges;
import io.deephaven.engine.v2.utils.singlerange.SingleRange;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.internal.log.LoggerFactory;
import org.jetbrains.annotations.NotNull;

public class TrackingMutableRowSetImpl extends GroupingRowSetHelper implements ImplementedByTreeIndexImpl, Externalizable {
    private static final long serialVersionUID = 3177210716109500229L;

    private TreeIndexImpl impl;
    private transient TreeIndexImpl prevImpl;
    /**
     * Protects prevImpl. Only updated in checkPrev() and initializePreviousValue() (this later supposed to be used only
     * right after the constructor, in special cases).
     */
    private transient volatile long changeTimeStep;

    @Override
    final public TreeIndexImpl getImpl() {
        return impl;
    }

    private final static boolean trace = false;
    private static final Logger log = LoggerFactory.getLogger(TrackingMutableRowSetImpl.class);

    private void checkPrevForWrite() {
        checkAndGetPrev();
    }

    private TreeIndexImpl checkAndGetPrev() {
        if (LogicalClock.DEFAULT.currentStep() == changeTimeStep) {
            return prevImpl;
        }
        synchronized (this) {
            final long currentClockStep = LogicalClock.DEFAULT.currentStep();
            if (currentClockStep == changeTimeStep) {
                return prevImpl;
            }
            prevImpl.ixRelease();
            prevImpl = impl.ixCowRef();
            changeTimeStep = currentClockStep;
            return prevImpl;
        }
    }

    private synchronized void initializePreviousValueInternal() {
        prevImpl.ixRelease();
        prevImpl = TreeIndexImpl.EMPTY;
        changeTimeStep = -1;
    }

    public static TrackingMutableRowSetImpl makeEmptyRsp() {
        return new TrackingMutableRowSetImpl(RspBitmap.makeEmpty());
    }

    public static TrackingMutableRowSetImpl makeEmptySr() {
        return new TrackingMutableRowSetImpl(SortedRanges.makeEmpty());
    }

    public static TrackingMutableRowSetImpl makeSingleRange(final long start, final long end) {
        return new TrackingMutableRowSetImpl(SingleRange.make(start, end));
    }

    @SuppressWarnings("unused")
    private void pre(final String op) {}

    private void pos() {}

    @SuppressWarnings("unused")
    private void pos(final Object ans) {}

    @SuppressWarnings("unused")
    public static void trace(String msg) {}

    public TrackingMutableRowSetImpl() {
        this(TreeIndexImpl.EMPTY);
    }

    public TrackingMutableRowSetImpl(final TreeIndexImpl impl) {
        this.impl = impl;
        this.prevImpl = TreeIndexImpl.EMPTY;
        changeTimeStep = -1;
    }

    @Override
    public TrackingMutableRowSet tracking() {
        return this;
    }

    @Override
    public void close() {
        impl.ixRelease();
        closeRowSequenceAsChunkImpl();
    }

    @VisibleForTesting
    @Override
    public int refCount() {
        return impl.ixRefCount();
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
    public void insert(final long key) {
        if (trace)
            pre("insert(" + key + ")");
        checkPrevForWrite();
        assign(impl.ixInsert(key));
        updateGroupingOnInsert(key);
        if (trace)
            pos();
    }

    @Override
    public void insertRange(final long startKey, final long endKey) {
        if (trace)
            pre("insertRange(" + startKey + ", " + endKey + ")");
        checkPrevForWrite();
        assign(impl.ixInsertRange(startKey, endKey));
        updateGroupOnInsertRange(startKey, endKey);
        if (trace)
            pos();
    }

    @Override
    public void insert(final LongChunk<Attributes.OrderedRowKeys> keys, final int offset, final int length) {
        Assert.leq(offset + length, "offset + length", keys.size(), "keys.size()");
        if (trace)
            pre("insert(chunk)");
        checkPrevForWrite();
        assign(impl.ixInsert(keys, offset, length));
        updateGroupingOnInsert(keys, offset, length);
        if (trace)
            pos();
    }

    @Override
    public void insert(final RowSet added) {
        if (trace)
            pre("insert(added_" + (added == null ? "id=-1" : ((ImplementedByTreeIndexImpl) added).strid()) + ")");
        if (added != null) {
            checkPrevForWrite();
            assign(impl.ixInsert(getImpl(added)));
            super.onInsert(added);
        }
        if (trace)
            pos();
    }

    @Override
    public void remove(final long key) {
        if (trace)
            pre("remove(" + key + ")");
        checkPrevForWrite();
        assign(impl.ixRemove(key));
        updateGroupingOnRemove(key);
        if (trace)
            pos();
    }

    @Override
    public void removeRange(final long startKey, final long endKey) {
        if (trace)
            pre("removeRange(" + startKey + ", " + endKey + ")");
        checkPrevForWrite();
        assign(impl.ixRemoveRange(startKey, endKey));
        updateGroupingOnRemoveRange(startKey, endKey);
        if (trace)
            pos();
    }

    @Override
    public void remove(final LongChunk<OrderedRowKeys> keys, final int offset, final int length) {
        Assert.leq(offset + length, "offset + length", keys.size(), "keys.size()");
        if (trace)
            pre("remove(chunk)");
        checkPrevForWrite();
        assign(impl.ixRemove(keys, offset, length));
        updateGroupingOnRemove(keys, offset, length);
        if (trace)
            pos();
    }

    @Override
    public void remove(final RowSet removed) {
        if (trace)
            pre("remove(removed_" + ((ImplementedByTreeIndexImpl) removed).strid() + ")");
        checkPrevForWrite();
        assign(impl.ixRemove(getImpl(removed)));
        super.onRemove(removed);
        if (trace)
            pos();
    }

    @Override
    public long lastRowKey() {
        if (trace)
            pre("lastRowKey");
        final long ans = impl.ixLastKey();
        if (trace)
            pos(ans);
        return ans;
    }

    @Override
    public long firstRowKey() {
        if (trace)
            pre("firstRowKey");
        final long ans = impl.ixFirstKey();
        if (trace)
            pos(ans);
        return ans;
    }

    @Override
    public void clear() {
        if (trace)
            pre("clear");
        checkPrevForWrite();
        impl.ixRelease();
        impl = TreeIndexImpl.EMPTY;
        onClear();
        if (trace)
            pos();
    }

    @Override
    public boolean forEachLong(LongAbortableConsumer lc) {
        if (trace)
            pre("forEachLong");
        final boolean r = impl.ixForEachLong(lc);
        if (trace)
            pos();
        return r;
    }

    @Override
    public boolean forEachLongRange(final LongRangeAbortableConsumer lrac) {
        if (trace)
            pre("forEachLongRange");
        final boolean r = impl.ixForEachLongRange(lrac);
        if (trace)
            pos();
        return r;
    }

    // endPos is exclusive.
    @Override
    public TrackingMutableRowSet subSetByPositionRange(final long startPos, final long endPos) {
        if (trace)
            pre("subSetByPositionRange(" + startPos + "," + endPos + ")");
        final TrackingMutableRowSetImpl ans = new TrackingMutableRowSetImpl(impl.ixSubindexByPosOnNew(startPos, endPos));
        if (trace)
            pos();
        return ans;
    }

    @Override
    public TrackingMutableRowSet subSetByKeyRange(final long startKey, final long endKey) {
        if (trace)
            pre("subSetByKeyRange(" + startKey + "," + endKey + ")");
        final TrackingMutableRowSetImpl ans = new TrackingMutableRowSetImpl(impl.ixSubindexByKeyOnNew(startKey, endKey));
        if (trace)
            pos();
        return ans;
    }

    @Override
    public long get(final long pos) {
        if (trace)
            pre("get(" + pos + ")");
        final long ans = impl.ixGet(pos);
        if (trace)
            pos(ans);
        return ans;
    }

    @Override
    public void getKeysForPositions(PrimitiveIterator.OfLong positions, LongConsumer outputKeys) {
        if (trace)
            pre("getKeysForPositions");
        impl.ixGetKeysForPositions(positions, outputKeys);
        if (trace)
            pos();
    }

    @Override
    public long getPrev(final long pos) {
        if (pos < 0) {
            return -1;
        }
        if (trace)
            pre("getPrev(" + pos + ")");
        final long ans = checkAndGetPrev().ixGet(pos);
        if (trace)
            pos();
        return ans;
    }

    @Override
    public long sizePrev() {
        if (trace)
            pre("sizePrev()");
        final long ans = checkAndGetPrev().ixCardinality();
        if (trace)
            pos();
        return ans;
    }

    @Override
    public TrackingMutableRowSet getPrevRowSet() {
        if (trace)
            pre("getPrevRowSet");
        final TreeIndexImpl r = checkAndGetPrev().ixCowRef();
        final TrackingMutableRowSet ans;
        ans = new TrackingMutableRowSetImpl(r);
        if (trace)
            pos();
        return ans;
    }

    @Override
    public void initializePreviousValue() {
        if (trace)
            pre("initializePreviousValue");
        initializePreviousValueInternal();
        if (trace)
            pos();
    }

    @Override
    public long find(long key) {
        if (trace)
            pre("find(" + key + ")");
        final long ans = impl.ixFind(key);
        if (trace)
            pos();
        return ans;
    }

    @Override
    public TrackingMutableRowSet invert(RowSet keys, long maximumPosition) {
        if (trace)
            pre("invert(" + keys + ")");
        final TreeIndexImpl result = impl.ixInvertOnNew(getImpl(keys), maximumPosition);
        final TrackingMutableRowSetImpl ans = new TrackingMutableRowSetImpl(result);
        if (trace)
            pos(ans);
        return ans;
    }

    @Override
    public long findPrev(long rowKey) {
        if (trace)
            pre("findPrev(" + rowKey + ")");
        final long ans = checkAndGetPrev().ixFind(rowKey);
        if (trace)
            pos();
        return ans;
    }

    @Override
    public long firstRowKeyPrev() {
        if (trace)
            pre("firstRowKeyPrev");
        final long ans = checkAndGetPrev().ixFirstKey();
        if (trace)
            pos();
        return ans;
    }

    @Override
    public long lastRowKeyPrev() {
        if (trace)
            pre("firstRowKeyPrev");
        final long ans = checkAndGetPrev().ixLastKey();
        if (trace)
            pos();
        return ans;
    }

    @Override
    public SearchIterator searchIterator() {
        if (trace)
            pre("searchIiterator");
        final SearchIterator ans = impl.ixSearchIterator();
        if (trace)
            pos();
        return ans;
    }

    @NotNull
    @Override
    public TrackingMutableRowSet.Iterator iterator() {
        if (trace)
            pre("iterator");
        final TrackingMutableRowSet.Iterator ans = impl.ixIterator();
        if (trace)
            pos();
        return ans;
    }

    @Override
    public SearchIterator reverseIterator() {
        if (trace)
            pre("reverseIterator");
        final SearchIterator ans = impl.ixReverseIterator();
        if (trace)
            pos();
        return ans;
    }

    @Override
    public RangeIterator rangeIterator() {
        if (trace)
            pre("rangeIterator");
        final RangeIterator ans = impl.ixRangeIterator();
        if (trace)
            pos();
        return ans;
    }

    @Override
    public long size() {
        if (trace)
            pre("size");
        final long ans = impl.ixCardinality();
        if (trace)
            pos();
        return ans;
    }

    @Override
    public boolean empty() {
        if (trace)
            pre("empty");
        final boolean ans = impl.ixIsEmpty();
        if (trace)
            pos();
        return ans;
    }

    @Override
    public boolean containsRange(final long start, final long end) {
        if (trace)
            pre("containsRange");
        final boolean ans = impl.ixContainsRange(start, end);
        if (trace)
            pos();
        return ans;
    }

    private static TreeIndexImpl getImpl(final RowSet index) {
        if (index instanceof ImplementedByTreeIndexImpl) {
            return ((ImplementedByTreeIndexImpl) index).getImpl();
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public void compact() {
        if (trace)
            pre("compact");
        assign(impl.ixCompact());
        if (trace)
            pos();
    }

    @Override
    public void update(final RowSet added, final RowSet removed) {
        if (trace)
            pre("update(added_" + ((ImplementedByTreeIndexImpl) added).strid() + ", removed_"
                    + ((ImplementedByTreeIndexImpl) removed).strid() + ")");
        checkPrevForWrite();
        assign(impl.ixUpdate(getImpl(added), getImpl(removed)));
        super.onUpdate(added, removed);
        if (trace)
            pos();
    }

    @Override
    public void retain(@NotNull final RowSet rowSetToIntersect) {
        if (trace)
            pre("retain(toIntersect_" + ((ImplementedByTreeIndexImpl) rowSetToIntersect).strid() + ")");
        checkPrevForWrite();
        assign(impl.ixRetain(getImpl(rowSetToIntersect)));
        super.onRetain(rowSetToIntersect);
        if (trace)
            pos();
    }

    @Override
    public void retainRange(final long startRowKey, final long endRowKey) {
        if (trace)
            pre("retainRange");
        checkPrevForWrite();
        assign(impl.ixRetainRange(startRowKey, endRowKey));
        updateGroupingOnRetainRange(startRowKey, endRowKey);
        if (trace)
            pos();
    }

    @NotNull
    @Override
    public TrackingMutableRowSet intersect(@NotNull final RowSet range) {
        if (trace)
            pre("intersect(range_" + ((ImplementedByTreeIndexImpl) range).strid());
        final GroupingRowSetHelper ans = new TrackingMutableRowSetImpl(impl.ixIntersectOnNew(getImpl(range)));
        if (trace)
            pos();
        return ans;
    }

    @Override
    public boolean overlaps(@NotNull final RowSet range) {
        if (trace)
            pre("overlaps");
        final boolean ans = impl.ixOverlaps(getImpl(range));
        if (trace)
            pos();
        return ans;
    }

    @Override
    public boolean overlapsRange(final long start, final long end) {
        if (trace)
            pre("overlapsRange");
        final boolean ans = impl.ixOverlapsRange(start, end);
        if (trace)
            pos();
        return ans;
    }

    @Override
    public boolean subsetOf(@NotNull final RowSet range) {
        if (trace)
            pre("subsetOf");
        final boolean ans = impl.ixSubsetOf(getImpl(range));
        if (trace)
            pos();
        return ans;
    }

    @Override
    public TrackingMutableRowSet minus(final RowSet set) {
        if (trace)
            pre("minus(set_" + ((ImplementedByTreeIndexImpl) set).strid());
        if (set == this) {
            return RowSetFactoryImpl.INSTANCE.getRowSetByValues();
        }
        final GroupingRowSetHelper ans = new TrackingMutableRowSetImpl(impl.ixMinusOnNew(getImpl(set)));
        if (trace)
            pos();
        return ans;
    }

    @Override
    public TrackingMutableRowSet union(final RowSet set) {
        if (trace)
            pre("union(set_" + ((ImplementedByTreeIndexImpl) set).strid());
        if (set == this) {
            return RowSetFactoryImpl.INSTANCE.getRowSetByValues();
        }
        final GroupingRowSetHelper ans = new TrackingMutableRowSetImpl(impl.ixUnionOnNew(getImpl(set)));
        if (trace)
            pos();
        return ans;
    }

    private static class RowSetRandomBuilder extends TreeIndexImplRandomBuilder implements RowSetBuilderRandom.RandomBuilder {
        @Override
        public MutableRowSet build() {
            return new TrackingMutableRowSetImpl(getTreeIndexImpl());
        }
    }

    public static RowSetBuilderRandom makeRandomBuilder() {
        return new RowSetRandomBuilder();
    }

    private abstract static class IndexBuilderSequentialBase extends TreeIndexImplSequentialBuilder
            implements RowSetBuilderSequential {
        @Override
        public void appendRowSet(final RowSet rowSet) {
            appendRowSetWithOffset(rowSet, 0);
        }

        @Override
        public void appendRowSetWithOffset(final RowSet rowSet, final long shiftAmount) {
            if (rowSet instanceof ImplementedByTreeIndexImpl) {
                appendTreeIndexImpl(shiftAmount, ((ImplementedByTreeIndexImpl) rowSet).getImpl(), false);
                return;
            }
            rowSet.forAllLongRanges((start, end) -> {
                appendRange(start + shiftAmount, end + shiftAmount);
            });
        }
    }

    private static class IndexBuilderSequential extends IndexBuilderSequentialBase {
        @Override
        public TrackingMutableRowSet build() {
            return new TrackingMutableRowSetImpl(getTreeIndexImpl());
        }
    }

    public static RowSetBuilderSequential makeSequentialBuilder() {
        return new IndexBuilderSequential();
    }

    private static class CurrentOnlyRowSetRandomBuilder extends TreeIndexImplRandomBuilder
            implements RowSetBuilderRandom.RandomBuilder {
        @Override
        public MutableRowSet build() {
            return new MutableRowSetImpl(getTreeIndexImpl());
        }
    }

    public static RowSetBuilderRandom makeCurrentRandomBuilder() {
        return new CurrentOnlyRowSetRandomBuilder();
    }

    private static class CurrentOnlyIndexBuilderSequential extends IndexBuilderSequentialBase {
        @Override
        public TrackingMutableRowSet build() {
            return new MutableRowSetImpl(getTreeIndexImpl());
        }
    }

    public static RowSetBuilderSequential makeCurrentSequentialBuilder() {
        return new CurrentOnlyIndexBuilderSequential();
    }

    public static GroupingRowSetHelper getEmptyIndex() {
        return new TrackingMutableRowSetImpl(TreeIndexImpl.EMPTY);
    }

    @Override
    public String toString() {
        return toString(200);
    }

    public String toString(int maxNodes) {
        return IndexUtilities.toString(this, maxNodes);
    }

    @Override
    public TrackingMutableRowSet shift(final long shiftAmount) {
        if (trace)
            pre("shift(" + shiftAmount + ")");
        final TrackingMutableRowSetImpl ans = new TrackingMutableRowSetImpl(impl.ixShiftOnNew(shiftAmount));
        if (trace)
            pos();
        return ans;
    }

    @Override
    public void shiftInPlace(final long shiftAmount) {
        if (trace)
            pre("shiftInPlace(" + shiftAmount + ")");
        assign(impl.ixShiftInPlace(shiftAmount));
        if (trace)
            pos();
    }

    @Override
    public void insertWithShift(final long shiftAmount, final RowSet other) {
        if (trace)
            pre("insertWithShift(" + shiftAmount + "," + other + ")");
        assign(impl.ixInsertWithShift(shiftAmount, getImpl(other)));
        if (trace)
            pos();
    }

    @Override
    public void validate(final String failMsg) {
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
            Assert.assertion(start > lastEnd, m + "start > lastEnd", start, "start", lastEnd, "lastEnd", this, "rowSet");
            Assert.assertion(start > lastEnd + 1, m + "start > lastEnd + 1", start, "start", lastEnd, "lastEnd", this,
                    "rowSet");
            lastEnd = end;

            totalSize += ((end - start) + 1);
        }

        Assert.eq(totalSize, m + "totalSize", size(), "size()");
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @Override
    public TrackingMutableRowSet clone() {
        if (trace)
            pre("clone");
        final TrackingMutableRowSetImpl ans = new TrackingMutableRowSetImpl(impl.ixCowRef());
        if (trace)
            pos();
        return ans;
    }

    public static void add(final TreeIndexImpl.RandomBuilder builder, final TrackingMutableRowSetImpl idx) {
        if (idx.impl instanceof SingleRange) {
            builder.add((SingleRange) idx.impl);
            return;
        }
        if (idx.impl instanceof SortedRanges) {
            builder.add((SortedRanges) idx.impl, true);
            return;
        }
        final RspBitmap idxImpl = (RspBitmap) idx.impl;
        builder.add(idxImpl, true);
    }

    @Override
    public LogOutput append(final LogOutput logOutput) {
        return IndexUtilities.append(logOutput, this.rangeIterator());
    }

    @Override
    public void writeExternal(@NotNull final ObjectOutput out) throws IOException {
        ExternalizableIndexUtils.writeExternalCompressedDeltas(out, this);
    }

    // If we've got a nasty bug, it can be useful to write the serialized version of indices when we detect the bug;
    // because the creation of these things is so darn path dependent. We can't actually serialize the TrackingMutableRowSet; because
    // the representation that we'll write will be completely different (and likely saner) than what we have in-memory
    // at any given point in time.
    public void writeImpl(ObjectOutput out) throws IOException {
        out.writeObject(impl);
    }

    @Override
    public void readExternal(@NotNull final ObjectInput in) throws IOException, ClassNotFoundException {
        try (final TrackingMutableRowSet readRowSet = ExternalizableIndexUtils.readExternalCompressedDelta(in)) {
            insert(readRowSet);
        }
    }

    //
    // From RowSequence
    //

    @Override
    public long getAverageRunLengthEstimate() {
        return impl.ixGetAverageRunLengthEstimate();
    }

    @Override
    public RowSequence.Iterator getRowSequenceIterator() {
        return impl.ixGetRowSequenceIterator();
    }

    @Override
    public RowSequence getRowSequenceByKeyRange(final long startRowKeyInclusive, final long endRowKeyInclusive) {
        return impl.ixGetRowSequenceByKeyRange(startRowKeyInclusive, endRowKeyInclusive);

    }

    @Override
    public RowSequence getRowSequenceByPosition(final long start, final long len) {
        return impl.ixGetRowSequenceByPosition(start, len);
    }

    @Override
    public TrackingMutableRowSet asIndex() {
        return this;
    }

    public void fillRowKeyChunk(final WritableLongChunk<? extends RowKeys> chunkToFill) {
        IndexUtilities.fillKeyIndicesChunk(this, chunkToFill);
    }

    @Override
    public void fillRowKeyRangesChunk(final WritableLongChunk<OrderedRowKeyRanges> chunkToFill) {
        IndexUtilities.fillKeyRangesChunk(this, chunkToFill);
    }

    @Override
    public long rangesCountUpperBound() {
        return impl.ixRangesCountUpperBound();
    }
}
