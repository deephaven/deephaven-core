//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import org.junit.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers {@link RowSetBuilderRandom#addRowSet(RowSet)}, which hands the row set's implementation to the inner builder
 * whole rather than walking it range by range, including where it is interleaved with key and range adds.
 */
public class RowSetBuilderRandomAddRowSetTest {

    /**
     * Independent of the builder under test: every row key, through a bit set and a sequential builder. Row keys must
     * fit in an int, which bounds the shapes these tests may use and fails loudly if one outgrows it.
     */
    private static WritableRowSet reference(final List<RowSet> rowSets) {
        final BitSet rowKeys = new BitSet();
        for (final RowSet rowSet : rowSets) {
            rowSet.forAllRowKeys(rowKey -> rowKeys.set(Math.toIntExact(rowKey)));
        }
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (int rowKey = rowKeys.nextSetBit(0); rowKey >= 0; rowKey = rowKeys.nextSetBit(rowKey + 1)) {
            builder.appendKey(rowKey);
        }
        return builder.build();
    }

    private static void closeAll(final List<RowSet> rowSets) {
        rowSets.forEach(RowSet::close);
    }

    private static void check(final List<RowSet> rowSets) {
        try (final WritableRowSet expected = reference(rowSets)) {
            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            for (final RowSet rowSet : rowSets) {
                builder.addRowSet(rowSet);
            }
            try (final WritableRowSet actual = builder.build()) {
                assertThat(actual).isEqualTo(expected);
            }
            // Adding in the reverse order must not change the result.
            final RowSetBuilderRandom reversed = RowSetFactory.builderRandom();
            for (int ii = rowSets.size() - 1; ii >= 0; --ii) {
                reversed.addRowSet(rowSets.get(ii));
            }
            try (final WritableRowSet actual = reversed.build()) {
                assertThat(actual).isEqualTo(expected);
            }
        }
    }

    @Test
    public void singleRangeAndSortedRangesAndRsp() {
        final List<RowSet> rowSets = new ArrayList<>();
        // SingleRange
        rowSets.add(RowSetFactory.fromRange(0, 99));
        // SortedRanges
        final RowSetBuilderSequential sorted = RowSetFactory.builderSequential();
        for (long key = 200; key < 400; key += 3) {
            sorted.appendKey(key);
        }
        rowSets.add(sorted.build());
        // Large enough to be an RspBitmap
        final RowSetBuilderSequential rsp = RowSetFactory.builderSequential();
        for (long key = 1_000; key < 2_000_000; key += 7) {
            rsp.appendKey(key);
        }
        rowSets.add(rsp.build());
        try {
            check(rowSets);
        } finally {
            closeAll(rowSets);
        }
    }

    @Test
    public void disjointOrdered() {
        final List<RowSet> rowSets = new ArrayList<>();
        for (int ii = 0; ii < 50; ++ii) {
            rowSets.add(RowSetFactory.fromRange(ii * 1000L, ii * 1000L + 499));
        }
        try {
            check(rowSets);
        } finally {
            closeAll(rowSets);
        }
    }

    @Test
    public void overlapping() {
        final List<RowSet> rowSets = new ArrayList<>();
        for (int ii = 0; ii < 50; ++ii) {
            rowSets.add(RowSetFactory.fromRange(ii * 10L, ii * 10L + 999));
        }
        try {
            check(rowSets);
        } finally {
            closeAll(rowSets);
        }
    }

    @Test
    public void emptyRowSetsAreIgnored() {
        try (
                final WritableRowSet empty = RowSetFactory.empty();
                final WritableRowSet nonEmpty = RowSetFactory.fromRange(5, 15)) {
            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            builder.addRowSet(empty);
            builder.addRowSet(nonEmpty);
            builder.addRowSet(empty);
            try (final WritableRowSet actual = builder.build()) {
                assertThat(actual).isEqualTo(nonEmpty);
            }
        }
    }

    @Test
    public void interleavedWithKeysAndRanges() {
        try (
                final WritableRowSet first = RowSetFactory.fromRange(100, 199);
                final WritableRowSet second = RowSetFactory.fromRange(500, 599)) {
            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            // Pending key and range state must be flushed before the row set is taken whole, and must keep working
            // afterwards.
            builder.addKey(7);
            builder.addRange(10, 20);
            builder.addRowSet(first);
            builder.addKey(300);
            builder.addRange(400, 410);
            builder.addRowSet(second);
            builder.addKey(1000);
            try (
                    final WritableRowSet actual = builder.build();
                    final WritableRowSet expected = RowSetFactory.fromKeys(7, 300, 1000)) {
                expected.insertRange(10, 20);
                expected.insert(first);
                expected.insertRange(400, 410);
                expected.insert(second);
                assertThat(actual).isEqualTo(expected);
            }
        }
    }

    @Test
    public void addRowSetFirstThenKeysBelowIt() {
        try (final WritableRowSet source = RowSetFactory.fromRange(1000, 1099)) {
            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            builder.addRowSet(source);
            builder.addKey(0);
            builder.addRange(500, 510);
            try (
                    final WritableRowSet actual = builder.build();
                    final WritableRowSet expected = RowSetFactory.fromKeys(0)) {
                expected.insertRange(500, 510);
                expected.insert(source);
                assertThat(actual).isEqualTo(expected);
            }
        }
    }

    @Test
    public void inputsAreNotModifiedAndResultIsIndependent() {
        try (
                final WritableRowSet first = RowSetFactory.fromRange(0, 99);
                final WritableRowSet second = RowSetFactory.fromRange(200, 299)) {
            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            builder.addRowSet(first);
            builder.addRowSet(second);
            try (final WritableRowSet actual = builder.build()) {
                actual.insert(10_000L);
                actual.removeRange(0, 49);
            }
            assertThat(first.size()).isEqualTo(100);
            assertThat(first.firstRowKey()).isEqualTo(0);
            assertThat(second.size()).isEqualTo(100);
        }
    }

    @Test
    public void randomized() {
        final Random random = new Random(20260914L);
        for (int trial = 0; trial < 100; ++trial) {
            final int n = 1 + random.nextInt(10);
            final List<RowSet> rowSets = new ArrayList<>(n);
            for (int ii = 0; ii < n; ++ii) {
                final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
                final int entries = random.nextInt(25);
                for (int jj = 0; jj < entries; ++jj) {
                    final long start = random.nextInt(2000);
                    if (random.nextBoolean()) {
                        builder.addRange(start, start + random.nextInt(50));
                    } else {
                        builder.addKey(start);
                    }
                }
                rowSets.add(builder.build());
            }
            try {
                check(rowSets);
            } finally {
                closeAll(rowSets);
            }
        }
    }
}
