package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.datastructures.LongRangeIterator;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;

import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;

public class AdaptiveOrderedLongSetBuilderRandom implements OrderedLongSet.BuilderRandom {

    private static final RowSetCounts rowSetCounts = new RowSetCounts("orderedLongSetBuilderRandom");

    private SortedRanges pendingSr = null;
    private long pendingRangeStart = -1;
    private long pendingRangeEnd = -1;
    private MixedBuilderRandom builder = null;

    private OrderedLongSet.BuilderRandom innerBuilder() {
        return builder;
    }

    private void setupInnerBuilderEmpty() {
        builder = new MixedBuilderRandom(2 * SortedRanges.MAX_CAPACITY);
    }

    private void setInnerBuilderNull() {
        builder = null;
    }

    @SuppressWarnings("UnusedReturnValue")
    private boolean flushPendingRange() {
        if (pendingRangeStart == -1) {
            return false;
        }
        if (innerBuilder() != null) {
            innerBuilder().addRange(pendingRangeStart, pendingRangeEnd);
        } else {
            if (pendingSr == null) {
                pendingSr = SortedRanges.makeSingleRange(pendingRangeStart, pendingRangeEnd);
            } else {
                return tryFlushToPendingSr();
            }
        }
        pendingRangeStart = pendingRangeEnd = -1;
        return true;
    }

    private boolean tryFlushToPendingSr() {
        final SortedRanges ans = pendingSr.addRangeUnsafe(pendingRangeStart, pendingRangeEnd);
        if (ans != null) {
            pendingSr = ans;
            pendingRangeStart = pendingRangeEnd = -1;
            return true;
        }
        flushPendingSrToInnerBuilder();
        innerBuilder().addRange(pendingRangeStart, pendingRangeEnd);
        pendingRangeStart = pendingRangeEnd = -1;
        return false;
    }

    private void flushPendingSrToInnerBuilder() {
        flushSrToInnerBuilder(pendingSr);
        pendingSr = null;
    }

    private void flushSrToInnerBuilder(final SortedRanges sr) {
        setupInnerBuilderEmpty();
        sr.forEachLongRange((final long start, final long end) -> {
            innerBuilder().appendRange(start, end);
            return true;
        });
    }

    private boolean tryMergeToPendingRange(final long firstKey, final long lastKey) {
        if (pendingRangeStart == -1) {
            pendingRangeStart = firstKey;
            pendingRangeEnd = lastKey;
            return true;
        }
        if (pendingRangeEnd < firstKey - 1 || lastKey < pendingRangeStart - 1) {
            return false;
        }
        pendingRangeStart = Math.min(pendingRangeStart, firstKey);
        pendingRangeEnd = Math.max(pendingRangeEnd, lastKey);
        return true;
    }

    private void newKey(final long key) {
        newRangeSafe(key, key);
    }

    private void newRange(final long firstKey, final long lastKey) {
        if (firstKey > lastKey) {
            throw new IllegalArgumentException("Illegal range start=" + firstKey + " > end=" + lastKey + ".");
        }
        newRangeSafe(firstKey, lastKey);
    }

    private void newRangeSafe(final long firstKey, final long lastKey) {
        if (tryMergeToPendingRange(firstKey, lastKey)) {
            return;
        }
        flushPendingRange();
        pendingRangeStart = firstKey;
        pendingRangeEnd = lastKey;
    }

    @Override
    public OrderedLongSet getTreeIndexImpl() {
        final OrderedLongSet ans;
        if (innerBuilder() == null && pendingSr == null) {
            if (pendingRangeStart == -1) {
                rowSetCounts.emptyCount.sample(1);
                ans = OrderedLongSet.EMPTY;
            } else {
                final SingleRange sr = SingleRange.make(pendingRangeStart, pendingRangeEnd);
                rowSetCounts.sampleSingleRange(sr);
                ans = sr;
                pendingRangeStart = -1;
            }
        } else {
            flushPendingRange();
            if (innerBuilder() == null) {
                pendingSr = pendingSr.tryCompactUnsafe(4);
                rowSetCounts.sampleSortedRanges(pendingSr);
                ans = pendingSr;
                pendingSr = null;
            } else {
                // No counts since they are gathered by inner builder.
                ans = innerBuilder().getTreeIndexImpl();
                setInnerBuilderNull();
            }
        }
        return ans;
    }

    @Override
    public void addKey(final long rowKey) {
        newKey(rowKey);
    }

    public void addKeys(final PrimitiveIterator.OfLong it) {
        final LongConsumer c = this::addKey;
        it.forEachRemaining(c);
    }

    @Override
    public void addRange(long firstRowKey, long lastRowKey) {
        newRange(firstRowKey, lastRowKey);
    }

    public void addRanges(final LongRangeIterator it) {
        it.forEachLongRange((final long start, final long end) -> {
            addRange(start, end);
            return true;
        });
    }

    public void addRowSet(final RowSet rowSet) {
        flushPendingRange();
        if (rowSet instanceof WritableRowSetImpl) {
            WritableRowSetImpl.addToBuilderFromImpl(this, (WritableRowSetImpl) rowSet);
            return;
        }
        rowSet.forEachRowKeyRange((final long start, final long end) -> {
            addRange(start, end);
            return true;
        });
    }

    @Override
    public void add(final SortedRanges ix, final boolean acquire) {
        builder.add(ix, acquire);
    }

    @Override
    public void add(final RspBitmap ix, final boolean acquire) {
        builder.add(ix, acquire);
    }
}
