//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers {@link RowSetAccumulator}: the union it builds, the keys it removes from other row sets, and the bound on how
 * many row sets it holds whatever order its inputs arrive in.
 */
public class RowSetAccumulatorTest {

    /** Keys a block apart, so that every key is its own span of a sparse row set. */
    private static final long SPARSE_STRIDE = 1L << 16;

    /** Assert that each held row set is more than twice the size of the next, and that the sizes add up. */
    private static void assertInvariant(final RowSetAccumulator accumulator) {
        final long[] sizes = accumulator.rowSetSizes();
        long total = 0;
        for (int ii = 0; ii < sizes.length; ++ii) {
            assertThat(sizes[ii]).isPositive();
            if (ii > 0) {
                assertThat(sizes[ii - 1]).isGreaterThan(2 * sizes[ii]);
            }
            total += sizes[ii];
        }
        assertThat(total).isEqualTo(accumulator.size());
        if (total > 0) {
            assertThat(sizes.length).isLessThanOrEqualTo(64 - Long.numberOfLeadingZeros(total));
        }
    }

    /** Single sparse keys, each a row set of its own, in the given order of positions. */
    private static List<RowSet> sparseKeys(final List<Integer> positions) {
        final List<RowSet> rowSets = new ArrayList<>(positions.size());
        for (final int position : positions) {
            rowSets.add(RowSetFactory.fromKeys(position * SPARSE_STRIDE));
        }
        return rowSets;
    }

    private static List<Integer> positions(final int count) {
        final List<Integer> positions = new ArrayList<>(count);
        for (int ii = 0; ii < count; ++ii) {
            positions.add(ii);
        }
        return positions;
    }

    private static void insertAndCheck(final List<RowSet> rowSets, final int expectedRowSetCount) {
        try (final RowSetAccumulator accumulator = new RowSetAccumulator();
                final WritableRowSet expected = RowSetFactory.empty()) {
            for (final RowSet rowSet : rowSets) {
                accumulator.insert(rowSet);
                expected.insert(rowSet);
                assertInvariant(accumulator);
            }
            if (expectedRowSetCount > 0) {
                assertThat(accumulator.rowSetSizes()).hasSize(expectedRowSetCount);
            }
            try (final RowSet built = accumulator.build()) {
                assertThat(built).isEqualTo(expected);
            }
            assertThat(accumulator.size()).isZero();
            assertThat(accumulator.rowSetSizes()).isEmpty();
        } finally {
            rowSets.forEach(RowSet::close);
        }
    }

    @Test
    public void testEmpty() {
        try (final RowSetAccumulator accumulator = new RowSetAccumulator()) {
            try (final RowSet empty = RowSetFactory.empty()) {
                accumulator.insert(empty);
            }
            assertThat(accumulator.size()).isZero();
            assertThat(accumulator.rowSetSizes()).isEmpty();
            try (final WritableRowSet rows = RowSetFactory.fromRange(0, 10)) {
                accumulator.removeFrom(rows);
                assertThat(rows.size()).isEqualTo(11);
            }
            try (final RowSet built = accumulator.build()) {
                assertThat(built.isEmpty()).isTrue();
            }
        }
    }

    @Test
    public void testAscendingAppendsToOneRowSet() {
        insertAndCheck(sparseKeys(positions(1000)), 1);
    }

    @Test
    public void testDescendingStaysLogarithmic() {
        final List<Integer> positions = positions(1000);
        Collections.reverse(positions);
        insertAndCheck(sparseKeys(positions), 0);
    }

    @Test
    public void testRandomOrderStaysLogarithmic() {
        final List<Integer> positions = positions(1000);
        Collections.shuffle(positions, new Random(0));
        insertAndCheck(sparseKeys(positions), 0);
    }

    @Test
    public void testLargeInsertAbsorbsSmallerRowSets() {
        final List<RowSet> rowSets = new ArrayList<>();
        // descending single keys leave several held row sets
        for (int ii = 20; ii > 10; --ii) {
            rowSets.add(RowSetFactory.fromKeys(ii * SPARSE_STRIDE));
        }
        // interleaved with all of them, so nothing appends; large enough to absorb everything
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (int ii = 0; ii < 100; ++ii) {
            builder.appendKey(ii * SPARSE_STRIDE / 4 + 1);
        }
        rowSets.add(builder.build());
        insertAndCheck(rowSets, 1);
    }

    @Test
    public void testAppendCarriesIntoLargerRowSet() {
        try (final RowSetAccumulator accumulator = new RowSetAccumulator();
                final RowSet low = RowSetFactory.fromRange(100, 199);
                final RowSet below = RowSetFactory.fromRange(0, 39);
                final RowSet above = RowSetFactory.fromRange(1000, 1029)) {
            accumulator.insert(low);
            accumulator.insert(below);
            assertThat(accumulator.rowSetSizes()).containsExactly(100, 40);
            // appends to the larger row set, which stays more than twice the size of the smaller one
            accumulator.insert(above);
            assertThat(accumulator.rowSetSizes()).containsExactly(130, 40);
            assertInvariant(accumulator);
            try (final RowSet between = RowSetFactory.fromRange(50, 54)) {
                // appends to the smaller row set only, which stays less than half the size of the larger
                accumulator.insert(between);
            }
            assertThat(accumulator.rowSetSizes()).containsExactly(130, 45);
            try (final RowSet between = RowSetFactory.fromRange(55, 79)) {
                // appends to the smaller row set, which is then more than half the size of the larger and carries
                accumulator.insert(between);
            }
            assertThat(accumulator.rowSetSizes()).containsExactly(200);
            final RowSetBuilderSequential expectedBuilder = RowSetFactory.builderSequential();
            expectedBuilder.appendRange(0, 39);
            expectedBuilder.appendRange(50, 79);
            expectedBuilder.appendRange(100, 199);
            expectedBuilder.appendRange(1000, 1029);
            try (final RowSet built = accumulator.build();
                    final RowSet expected = expectedBuilder.build()) {
                assertThat(built).isEqualTo(expected);
            }
        }
    }

    @Test
    public void testRemoveFrom() {
        final Random random = new Random(0);
        final List<Integer> positions = positions(2000);
        Collections.shuffle(positions, random);
        try (final RowSetAccumulator accumulator = new RowSetAccumulator();
                final WritableRowSet expected = RowSetFactory.empty()) {
            for (int ii = 0; ii < positions.size(); ii += 10) {
                // a row set of up to ten sparse keys and one dense range, none of them held yet
                final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
                for (int jj = ii; jj < Math.min(ii + 10, positions.size()); ++jj) {
                    builder.addKey(positions.get(jj) * SPARSE_STRIDE);
                    builder.addRange(positions.get(jj) * SPARSE_STRIDE + 2, positions.get(jj) * SPARSE_STRIDE + 50);
                }
                try (final RowSet rows = builder.build()) {
                    accumulator.insert(rows);
                    expected.insert(rows);
                }
                assertInvariant(accumulator);

                // a probe that overlaps held and unheld keys
                final RowSetBuilderRandom probeBuilder = RowSetFactory.builderRandom();
                for (int jj = 0; jj < 20; ++jj) {
                    final long position = random.nextInt(positions.size());
                    probeBuilder.addRange(position * SPARSE_STRIDE, position * SPARSE_STRIDE + random.nextInt(60));
                }
                try (final WritableRowSet probe = probeBuilder.build();
                        final RowSet expectedRemaining = probe.minus(expected)) {
                    accumulator.removeFrom(probe);
                    assertThat(probe).isEqualTo(expectedRemaining);
                }
            }
            try (final RowSet built = accumulator.build()) {
                assertThat(built).isEqualTo(expected);
            }
        }
    }
}
