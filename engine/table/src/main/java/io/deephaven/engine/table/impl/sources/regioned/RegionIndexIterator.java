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
        return new RegionIndexIterator(sit, nextRegion(sit, 0));
    }

    private static final int DONE_REGION = -1;

    private final RowSet.SearchIterator sit;
    private int nextRegion;

    private static int nextRegion(final RowSet.SearchIterator sit, final long key) {
        return sit.advance(key)
                ? RegionedColumnSource.getRegionIndex(sit.currentValue())
                : DONE_REGION;
    }

    private RegionIndexIterator(final RowSet.SearchIterator sit, final int nextRegion) {
        this.sit = Objects.requireNonNull(sit);
        this.nextRegion = nextRegion;
    }

    @Override
    public boolean hasNext() {
        return nextRegion >= 0;
    }

    @Override
    public int nextInt() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final int currentRegion = nextRegion;
        final long currentRegionLastRowKey = RegionedColumnSource.getLastRowKey(nextRegion);
        nextRegion = currentRegionLastRowKey == Long.MAX_VALUE
                ? DONE_REGION
                : nextRegion(sit, currentRegionLastRowKey + 1);
        return currentRegion;
    }

    @Override
    public void close() {
        sit.close();
    }
}
