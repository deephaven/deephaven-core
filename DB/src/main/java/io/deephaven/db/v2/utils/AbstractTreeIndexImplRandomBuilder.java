package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.utils.rsp.RspBitmap;
import io.deephaven.db.v2.utils.singlerange.SingleRange;
import io.deephaven.db.v2.utils.sortedranges.SortedRanges;

import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;

public abstract class AbstractTreeIndexImplRandomBuilder implements TreeIndexImpl.RandomBuilder {
    protected static final IndexCounts indexCounts = new IndexCounts("randomIndexBuilder");

    protected SortedRanges pendingSr = null;
    private long pendingRangeStart = -1;
    private long pendingRangeEnd = -1;

    protected abstract TreeIndexImpl.RandomBuilder innerBuilder();

    protected abstract void setupInnerBuilderForRange(final long start, final long end);

    protected abstract void setupInnerBuilderEmpty();

    protected abstract void setInnerBuilderNull();

    protected boolean flushPendingRange() {
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

    protected void flushPendingSrToInnerBuilder() {
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
            if (Index.BAD_RANGES_AS_ERROR) {
                throw new IllegalArgumentException(
                    "Illegal range start=" + firstKey + " > end=" + lastKey + ".");
            }
            // Ignore.
            return;
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
    public TreeIndexImpl getTreeIndexImpl() {
        final TreeIndexImpl ans;
        if (innerBuilder() == null && pendingSr == null) {
            if (pendingRangeStart == -1) {
                indexCounts.emptyCount.sample(1);
                ans = TreeIndexImpl.EMPTY;
            } else {
                final SingleRange sr = SingleRange.make(pendingRangeStart, pendingRangeEnd);
                indexCounts.sampleSingleRange(sr);
                ans = sr;
                pendingRangeStart = -1;
            }
        } else {
            flushPendingRange();
            if (innerBuilder() == null) {
                pendingSr = pendingSr.tryCompactUnsafe(4);
                indexCounts.sampleSortedRanges(pendingSr);
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
    public void addKey(final long key) {
        newKey(key);
    }

    public void addKeys(final PrimitiveIterator.OfLong it) {
        final LongConsumer c = this::addKey;
        it.forEachRemaining(c);
    }

    @Override
    public void addRange(long firstKey, long lastKey) {
        newRange(firstKey, lastKey);
    }

    public void addRanges(final LongRangeIterator it) {
        it.forEachLongRange((final long start, final long end) -> {
            addRange(start, end);
            return true;
        });
    }

    public void addIndex(final Index idx) {
        flushPendingRange();
        if (idx instanceof TreeIndex) {
            TreeIndex.add(this, (TreeIndex) idx);
            return;
        }
        idx.forEachLongRange((final long start, final long end) -> {
            addRange(start, end);
            return true;
        });
    }

    @Override
    public void add(final SortedRanges other, final boolean acquire) {
        flushPendingRange();
        if (pendingSr != null) {
            final TreeIndexImpl tix = pendingSr.insertImpl(other, false);
            if (tix instanceof SortedRanges) {
                pendingSr = (SortedRanges) tix;
                return;
            }
            pendingSr = null;
            if (innerBuilder() == null) {
                setupInnerBuilderEmpty();
            }
            innerBuilder().add((RspBitmap) tix, true);
            return;
        }
        if (innerBuilder() == null) {
            setupInnerBuilderEmpty();
        }
        innerBuilder().add(other, acquire);
    }

    @Override
    public void add(final RspBitmap other, final boolean acquire) {
        flushPendingRange();
        if (pendingSr == null && innerBuilder() == null) {
            setupInnerBuilderEmpty();
            innerBuilder().add(other, acquire);
            return;
        }
        if (pendingSr != null) {
            flushPendingSrToInnerBuilder();
        }
        innerBuilder().add(other, acquire);
    }
}
