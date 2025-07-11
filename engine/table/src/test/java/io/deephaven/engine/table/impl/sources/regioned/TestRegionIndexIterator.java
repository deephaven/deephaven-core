//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;


import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import java.util.NoSuchElementException;

import static io.deephaven.engine.rowset.RowSetFactory.fromKeys;
import static io.deephaven.engine.rowset.RowSetFactory.fromRange;
import static io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource.getFirstRowKey;
import static io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource.getLastRowKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class TestRegionIndexIterator {

    @Test
    public void empty() {
        try (final WritableRowSet empty = RowSetFactory.empty()) {
            expect(empty);
        }
    }

    @Test
    public void region_0_startKey() {
        try (final WritableRowSet rowSet = fromKeys(getFirstRowKey(0))) {
            expect(rowSet, 0);
        }
    }

    @Test
    public void region_0_endKey() {
        try (final WritableRowSet rowSet = fromKeys(getLastRowKey(0))) {
            expect(rowSet, 0);
        }
    }

    @Test
    public void region_0_full() {
        try (final WritableRowSet rowSet = fromRange(getFirstRowKey(0), getLastRowKey(0))) {
            expect(rowSet, 0);
        }
    }

    @Test
    public void region_0_fullPlusOne() {
        try (final WritableRowSet rowSet = fromRange(getFirstRowKey(0), getLastRowKey(0) + 1)) {
            expect(rowSet, 0, 1);
        }
    }

    @Test
    public void region_1_3_5_full() {
        try (final WritableRowSet rowSet = RowSetFactory.empty()) {
            rowSet.insertRange(getFirstRowKey(1), getLastRowKey(1));
            rowSet.insertRange(getFirstRowKey(3), getLastRowKey(3));
            rowSet.insertRange(getFirstRowKey(5), getLastRowKey(5));
            expect(rowSet, 1, 3, 5);
        }
    }

    @Test
    public void region_1_10_100_1000_10000_100000_1000000_mixedStartKeyEndKey() {
        try (final WritableRowSet rowSet = fromKeys(
                getFirstRowKey(1),
                getLastRowKey(10),
                getFirstRowKey(100),
                getLastRowKey(1000),
                getFirstRowKey(10000),
                getLastRowKey(100000),
                getFirstRowKey(1000000))) {
            expect(rowSet, 1, 10, 100, 1000, 10000, 100000, 1000000);
        }
    }

    @Test
    public void lastRegion() {
        try (final WritableRowSet rowSet = fromKeys(Long.MAX_VALUE)) {
            expect(rowSet, RegionedColumnSource.MAXIMUM_REGION_COUNT - 1);
        }
    }

    private void expect(RowSet rowSet, int... expectedRegionIndices) {
        try (final RegionIndexIterator it = RegionIndexIterator.of(rowSet)) {
            for (final int expected : expectedRegionIndices) {
                assertThat(it).hasNext();
                assertThat(it.nextInt()).isEqualTo(expected);
            }
            assertThat(it).isExhausted();
            expectNextIntNoSuchElement(it);
        }
    }

    private static void expectNextIntNoSuchElement(RegionIndexIterator rit) {
        try {
            rit.nextInt();
            failBecauseExceptionWasNotThrown(NoSuchElementException.class);
        } catch (NoSuchElementException e) {
            // expected
        }
    }
}
