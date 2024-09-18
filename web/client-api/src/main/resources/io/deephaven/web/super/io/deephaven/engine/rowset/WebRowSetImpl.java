package io.deephaven.engine.rowset;

import io.deephaven.util.datastructures.LongAbortableConsumer;
import io.deephaven.util.datastructures.LongRangeConsumer;
import io.deephaven.web.shared.data.RangeSet;

import java.util.PrimitiveIterator;

final class WebRowSetImpl implements RowSet, WritableRowSet {
    private final RangeSet rangeSet;

    WebRowSetImpl(RangeSet rangeSet) {
        this.rangeSet = rangeSet;
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
        throw new UnsupportedOperationException("intersect");
    }
    @Override
    public WritableRowSet shift(long shiftAmount) {
        throw new UnsupportedOperationException("shift");
    }


    @Override
    public long size() {
        return rangeSet.size();
    }

    @Override
    public void close() {

    }

    @Override
    public RowSet copy() {
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
