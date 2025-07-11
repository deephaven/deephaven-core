//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.rowset.RowSet;

import java.io.Closeable;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PrimitiveIterator;

final class RegionIndexIterator implements PrimitiveIterator.OfInt, Closeable {

    /**
     * Creates an iterator of the in-order {@link RegionedColumnSource#getRegionIndex(long) region indices} from
     * {@code rowSet}.
     *
     * @param rowSet the row set
     * @return the region index iterator
     */
    public static RegionIndexIterator of(final RowSet rowSet) {
        // Note: we are doing an up-front check, with our implementation doing the follow-up hasNext check as part of
        // the nextInt calculation. This allows us to be efficient and call SearchIterator#advance and
        // SearchIterator#currentValue exactly once without needing to otherwise keep more complicated state handling
        // if hasNext was on-demand.
        final RowSet.SearchIterator sit = rowSet.searchIterator();
        return new RegionIndexIterator(sit, sit.advance(0) ? sit.currentValue() : DONE_KEY);
    }

    private static final int DONE_KEY = -1;

    private final RowSet.SearchIterator sit;
    private long currentKey;

    private RegionIndexIterator(final RowSet.SearchIterator sit, final long currentKey) {
        this.sit = Objects.requireNonNull(sit);
        this.currentKey = currentKey;
    }

    @Override
    public boolean hasNext() {
        return currentKey >= 0;
    }

    @Override
    public int nextInt() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final int regionIndex = RegionedColumnSource.getRegionIndex(currentKey);
        final long regionLastRowKey = RegionedColumnSource.getLastRowKey(regionIndex);
        if (regionLastRowKey == Long.MAX_VALUE || !sit.advance(regionLastRowKey + 1)) {
            currentKey = DONE_KEY;
        } else {
            currentKey = sit.currentValue();
        }
        return regionIndex;
    }

    @Override
    public void close() {
        sit.close();
    }
}
