package io.deephaven.engine.rowset;

import io.deephaven.util.datastructures.LongAbortableConsumer;
import io.deephaven.util.datastructures.LongRangeConsumer;
import io.deephaven.web.shared.data.Range;
import io.deephaven.web.shared.data.RangeSet;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.PrimitiveIterator;

final class WebRowSetImpl implements RowSet, WritableRowSet {
    private final RangeSet rangeSet;

    WebRowSetImpl(RangeSet rangeSet) {
        this.rangeSet = rangeSet;
    }

    @Override
    public void insert(long key) {
        rangeSet.addRange(new Range(key, key));
    }

    @Override
    public void insert(RowSet added) {
        rangeSet.addRangeSet(((WebRowSetImpl)added).rangeSet);
    }

    @Override
    public boolean isEmpty() {
        return rangeSet.isEmpty();
    }

    @Override
    public long lastRowKey() {
        return rangeSet.getLastRow();
    }

    @Override
    public boolean forEachRowKey(LongAbortableConsumer lac) {
        PrimitiveIterator.OfLong iter = rangeSet.indexIterator();
        while (iter.hasNext()) {
            long key = iter.nextLong();
            if (!lac.accept(key)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void forAllRowKeyRanges(LongRangeConsumer lrc) {
        rangeSet.rangeIterator().forEachRemaining(r -> {
            lrc.accept(r.getFirst(), r.getLast());
        });
    }

    @Override
    public long get(long position) {
        return rangeSet.get(position);
    }

    @Override
    public WritableRowSet intersect(RowSet rowSet) {
        if (rowSet.equals(this)) {
            return copy();
        }
        if (this.isEmpty() || rowSet.isEmpty()) {
            return RowSetFactory.empty();
        }
        throw new UnsupportedOperationException("intersect");
    }

    @Override
    public boolean overlapsRange(long start, long end) {
        throw new UnsupportedOperationException("overlapsRange");
    }

    @Override
    public boolean subsetOf(@NotNull RowSet other) {
        throw new UnsupportedOperationException("subsetOf");
    }

    @Override
    public void removeRange(long startKey, long endKey) {
        rangeSet.removeRange(new Range(startKey, endKey));
    }

    @Override
    public void remove(RowSet removed) {
        rangeSet.removeRangeSet(((WebRowSetImpl) removed).rangeSet);
    }

    @Override
    public RowSequence.Iterator getRowSequenceIterator() {
        throw new UnsupportedOperationException("getRowSequenceIterator");
    }

    @Override
    public SearchIterator searchIterator() {
        throw new UnsupportedOperationException("searchIterator");
    }

    @Override
    public SearchIterator reverseIterator() {
        throw new UnsupportedOperationException("reverseIterator");
    }

    @Override
    public RangeIterator rangeIterator() {
        java.util.Iterator<Range> iter = rangeSet.rangeIterator();
        return new RangeIterator() {
            private Range current;
            @Override
            public void close() {

            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public boolean advance(long v) {
                throw new UnsupportedOperationException("advance");
            }

            @Override
            public void postpone(long v) {
                throw new UnsupportedOperationException("postpone");
            }

            @Override
            public long currentRangeStart() {
                return current.getFirst();
            }

            @Override
            public long currentRangeEnd() {
                return current.getLast();
            }

            @Override
            public long next() {
                current = iter.next();
                return currentRangeStart();
            }
        };
    }

    @Override
    public WritableRowSet shift(long shiftAmount) {
        throw new UnsupportedOperationException("shift");
    }

    @Override
    public void shiftInPlace(long shiftAmount) {
        throw new UnsupportedOperationException("shiftInPlace");
    }

    @Override
    public WritableRowSet minus(RowSet rowSetToRemove) {
        throw new UnsupportedOperationException("minus");
    }

    @Override
    public WritableRowSet invert(RowSet keys, long maximumPosition) {
        throw new UnsupportedOperationException("invert");
    }

    @Override
    public void retain(RowSet rowSetToIntersect) {
        throw new UnsupportedOperationException("retain");
    }

    @Override
    public WritableRowSet subSetByKeyRange(long startKey, long endKey) {
        throw new UnsupportedOperationException("subSetByKeyRange");
    }

    @Override
    public long size() {
        return rangeSet.size();
    }

    @Override
    public void close() {

    }

    @Override
    public WritableRowSet copy() {
        return new WebRowSetImpl(rangeSet.copy());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof WebRowSetImpl)) {
            return false;
        }
        return rangeSet.equals(((WebRowSetImpl) obj).rangeSet);
    }

    @Override
    public int hashCode() {
        return rangeSet.hashCode();
    }
}
