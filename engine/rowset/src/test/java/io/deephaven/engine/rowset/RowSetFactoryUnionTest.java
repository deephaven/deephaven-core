//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers {@link RowSetFactory#union} over the input shapes that select different merge strategies.
 */
public class RowSetFactoryUnionTest {

    /**
     * Independent of everything this change touches: every row key, through a bit set and a sequential builder. Row
     * keys must fit in an int, which bounds the shapes these tests may use and fails loudly if one outgrows it.
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
        for (final RowSet rowSet : rowSets) {
            rowSet.close();
        }
    }

    /**
     * Assert that both input forms produce the union and leave their inputs intact.
     */
    private static void check(final List<RowSet> rowSets) {
        try (final WritableRowSet expected = reference(rowSets)) {
            final long[] sizesBefore = rowSets.stream().mapToLong(RowSet::size).toArray();

            try (final WritableRowSet actual = RowSetFactory.union(rowSets)) {
                assertThat(actual).isEqualTo(expected);
            }
            try (final WritableRowSet actual = RowSetFactory.union(rowSets.toArray(RowSet[]::new))) {
                assertThat(actual).isEqualTo(expected);
            }
            // Inputs are untouched by the non-consuming form.
            assertThat(rowSets.stream().mapToLong(RowSet::size).toArray()).isEqualTo(sizesBefore);

        }
    }

    // ------------------------------------------------------------------------------------------------
    // Shapes. Each one selects a different path through the merge.
    // ------------------------------------------------------------------------------------------------

    /** Disjoint blocks laid end to end: every insert appends, so one pass consumes all of them. */
    private static List<RowSet> orderedDisjoint(final int n) {
        final List<RowSet> rowSets = new ArrayList<>(n);
        for (int ii = 0; ii < n; ++ii) {
            rowSets.add(RowSetFactory.fromRange(ii * 100L, ii * 100L + 49));
        }
        return rowSets;
    }

    /** Disjoint sets that all span the key space: nothing appends and nothing duplicates, so this pairs. */
    private static List<RowSet> interleaved(final int n, final int rangesPerSet) {
        final RowSetBuilderSequential[] builders = new RowSetBuilderSequential[n];
        for (int ii = 0; ii < n; ++ii) {
            builders[ii] = RowSetFactory.builderSequential();
        }
        for (int range = 0; range < n * rangesPerSet; ++range) {
            final long start = range * 10L;
            builders[range % n].appendRange(start, start + 4);
        }
        final List<RowSet> rowSets = new ArrayList<>(n);
        for (int ii = 0; ii < n; ++ii) {
            rowSets.add(builders[ii].build());
        }
        return rowSets;
    }

    /** Disjoint sets whose ranges abut: no row is duplicated even though the union is contiguous. */
    private static List<RowSet> adjacent(final int n, final int rangesPerSet) {
        final RowSetBuilderSequential[] builders = new RowSetBuilderSequential[n];
        for (int ii = 0; ii < n; ++ii) {
            builders[ii] = RowSetFactory.builderSequential();
        }
        for (int range = 0; range < n * rangesPerSet; ++range) {
            final long start = range * 10L;
            builders[range % n].appendRange(start, start + 9);
        }
        final List<RowSet> rowSets = new ArrayList<>(n);
        for (int ii = 0; ii < n; ++ii) {
            rowSets.add(builders[ii].build());
        }
        return rowSets;
    }

    /** Every set covers the same span, so the accumulator saturates and insertion stays cheap. */
    private static List<RowSet> redundant(final int n) {
        final List<RowSet> rowSets = new ArrayList<>(n);
        for (int ii = 0; ii < n; ++ii) {
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            for (long key = ii; key < 2000; key += 3) {
                builder.appendKey(key);
            }
            rowSets.add(builder.build());
        }
        return rowSets;
    }

    /**
     * A pair that shares rows, then a long run of mutually disjoint sets that interleave through the key space. The
     * merge must not treat the early overlap as licence to keep absorbing the disjoint tail.
     */
    private static List<RowSet> overlapThenDisjoint(final int n) {
        final List<RowSet> rowSets = new ArrayList<>(n);
        rowSets.add(RowSetFactory.fromRange(0, 99));
        rowSets.add(RowSetFactory.fromRange(50, 149));
        final RowSetBuilderSequential[] builders = new RowSetBuilderSequential[n - 2];
        for (int ii = 0; ii < n - 2; ++ii) {
            builders[ii] = RowSetFactory.builderSequential();
        }
        for (int range = 0; range < (n - 2) * 7; ++range) {
            final long start = 1000L + range * 10L;
            builders[range % (n - 2)].appendRange(start, start + 4);
        }
        for (final RowSetBuilderSequential builder : builders) {
            rowSets.add(builder.build());
        }
        return rowSets;
    }

    /** Successive sets overlap halfway into the previous one. */
    private static List<RowSet> partialOverlap(final int n) {
        final List<RowSet> rowSets = new ArrayList<>(n);
        for (int ii = 0; ii < n; ++ii) {
            rowSets.add(RowSetFactory.fromRange(ii * 50L, ii * 50L + 99));
        }
        return rowSets;
    }

    private static void checkAndClose(final List<RowSet> rowSets) {
        try {
            check(rowSets);
        } finally {
            closeAll(rowSets);
        }
    }

    @Test
    public void empty() {
        try (final WritableRowSet actual = RowSetFactory.union(List.of())) {
            assertThat(actual.isEmpty()).isTrue();
        }
        try (final WritableRowSet actual = RowSetFactory.union()) {
            assertThat(actual.isEmpty()).isTrue();
        }
        try (
                final WritableRowSet r1 = RowSetFactory.empty();
                final WritableRowSet r2 = RowSetFactory.empty();
                final WritableRowSet actual = RowSetFactory.union(List.of(r1, r2))) {
            assertThat(actual.isEmpty()).isTrue();
        }
    }

    @Test
    public void emptiesAreSkipped() {
        try (
                final WritableRowSet empty = RowSetFactory.empty();
                final WritableRowSet nonEmpty = RowSetFactory.fromRange(0, 9);
                final WritableRowSet actual = RowSetFactory.union(List.of(empty, nonEmpty, empty))) {
            assertThat(actual).isEqualTo(nonEmpty);
        }
    }

    @Test
    public void single() {
        try (
                final WritableRowSet r1 = RowSetFactory.fromRange(0, 9);
                final WritableRowSet actual = RowSetFactory.union(List.of(r1))) {
            assertThat(actual).isEqualTo(r1);
            assertThat(actual).isNotSameAs(r1);
        }
    }

    @Test
    public void shapes() {
        checkAndClose(orderedDisjoint(41));
        checkAndClose(interleaved(17, 7));
        checkAndClose(adjacent(17, 7));
        checkAndClose(redundant(23));
        checkAndClose(partialOverlap(31));
        checkAndClose(overlapThenDisjoint(23));
        // Two row sets is the smallest input that forms a group with a partner.
        checkAndClose(orderedDisjoint(2));
        checkAndClose(interleaved(2, 5));
        // An odd count leaves a group of one at the tail of a pass.
        checkAndClose(orderedDisjoint(3));
        checkAndClose(interleaved(3, 5));
    }

    @Test
    public void presentationOrderDoesNotMatter() {
        for (final int n : new int[] {2, 3, 17, 41}) {
            final List<RowSet> ascending = orderedDisjoint(n);
            try {
                final List<RowSet> descending = new ArrayList<>(ascending);
                Collections.reverse(descending);
                final List<RowSet> shuffled = new ArrayList<>(ascending);
                Collections.shuffle(shuffled, new Random(n));
                checkSameResult(ascending, descending);
                checkSameResult(ascending, shuffled);
                check(descending);
                check(shuffled);
            } finally {
                closeAll(ascending);
            }
        }
    }

    private static void checkSameResult(final List<RowSet> lhs, final List<RowSet> rhs) {
        try (
                final WritableRowSet lhsResult = RowSetFactory.union(lhs);
                final WritableRowSet rhsResult = RowSetFactory.union(rhs)) {
            assertThat(lhsResult).isEqualTo(rhsResult);
        }
    }

    @Test
    public void resultIsIndependentOfInputs() {
        final List<RowSet> rowSets = orderedDisjoint(9);
        try {
            final long[] sizesBefore = rowSets.stream().mapToLong(RowSet::size).toArray();
            try (final WritableRowSet actual = RowSetFactory.union(rowSets)) {
                actual.insert(1_000_000L);
                actual.remove(0L);
            }
            assertThat(rowSets.stream().mapToLong(RowSet::size).toArray()).isEqualTo(sizesBefore);
            assertThat(rowSets.get(0).containsRange(0, 49)).isTrue();
        } finally {
            closeAll(rowSets);
        }
    }

    @Test
    public void randomized() {
        final Random random = new Random(20260914L);
        for (int trial = 0; trial < 60; ++trial) {
            final int n = 1 + random.nextInt(12);
            final List<RowSet> rowSets = new ArrayList<>(n);
            for (int ii = 0; ii < n; ++ii) {
                final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
                final int entries = random.nextInt(20);
                for (int jj = 0; jj < entries; ++jj) {
                    final long start = random.nextInt(500);
                    if (random.nextBoolean()) {
                        builder.addRange(start, start + random.nextInt(20));
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

    @Test
    public void deprecatedUnionInsertStillUnions() {
        try (
                final WritableRowSet r1 = RowSetFactory.fromRange(0, 9);
                final WritableRowSet r2 = RowSetFactory.fromRange(20, 29);
                final WritableRowSet expected = r1.union(r2);
                @SuppressWarnings("deprecation")
                final WritableRowSet actual = RowSetFactory.unionInsert(List.of(r1, r2))) {
            assertThat(actual).isEqualTo(expected);
        }
    }
}
