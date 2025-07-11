//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.rowset.RowSet;

import java.io.Closeable;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.function.IntConsumer;

final class RegionIndexIterator implements PrimitiveIterator.OfInt, Closeable {

    /**
     * Creates an iterator of the in-order {@link RegionedColumnSource#getRegionIndex(long) region indices} from
     * {@code rowSet}.
     *
     * @param rowSet the row set
     * @return the region index iterator
     */
    public static RegionIndexIterator of(final RowSet rowSet) {
        return new RegionIndexIterator(rowSet.searchIterator());
    }

    private final RowSet.SearchIterator sit;
    private long key;

    private RegionIndexIterator(final RowSet.SearchIterator sit) {
        this.sit = Objects.requireNonNull(sit);
        this.key = 0;
    }

    @Override
    public boolean hasNext() {
        return sit.advance(key);
    }

    @Override
    public int nextInt() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return nextRegionIndexUnchecked();
    }

    /**
     * The next region index. The caller must have already verified {@link #hasNext()} is {@code true}.
     *
     * @return the next region index
     */
    public int nextRegionIndexUnchecked() {
        final long regionStartKey = sit.currentValue();
        final int regionIndex = RegionedColumnSource.getRegionIndex(regionStartKey);
        key = RegionedColumnSource.getLastRowKey(regionIndex) + 1;
        return regionIndex;
    }

    @Override
    public void forEachRemaining(IntConsumer action) {
        while (hasNext()) {
            action.accept(nextRegionIndexUnchecked());
        }
    }

    @Override
    public void close() {
        sit.close();
    }
}
