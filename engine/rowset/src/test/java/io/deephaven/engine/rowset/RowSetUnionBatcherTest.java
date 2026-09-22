//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.util.SafeCloseable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers {@link RowSetUnionBatcher}: what it builds, what it does with the row sets it is handed, and the two things it
 * does before the merge sees them, which are dropping empty row sets and collapsing runs of appends.
 *
 * <p>
 * Every row set handed to {@link RowSetUnionBatcher#add(WritableRowSet)} is owned by it, so a test that keeps its
 * inputs to compare against hands over a {@link RowSet#copy() copy}.
 */
public class RowSetUnionBatcherTest {

    /**
     * Independent of everything under test: every row key, through a bit set and a sequential builder. Row keys must
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

    /** Disjoint blocks laid end to end. Every one of these appends to the one before it. */
    private static List<RowSet> ascendingDisjoint(final int count) {
        final List<RowSet> rowSets = new ArrayList<>(count);
        for (int ii = 0; ii < count; ++ii) {
            rowSets.add(RowSetFactory.fromRange(ii * 100L, ii * 100L + 49));
        }
        return rowSets;
    }

    /**
     * Disjoint sets that all span the key space, so nothing appends to anything. Multiple ranges each, which is what
     * gives them a reference-counted implementation to observe.
     */
    private static List<RowSet> interleaved(final int count, final int rangesPerSet) {
        final RowSetBuilderSequential[] builders = new RowSetBuilderSequential[count];
        for (int ii = 0; ii < count; ++ii) {
            builders[ii] = RowSetFactory.builderSequential();
        }
        for (int range = 0; range < count * rangesPerSet; ++range) {
            final long start = range * 10L;
            builders[range % count].appendRange(start, start + 4);
        }
        final List<RowSet> rowSets = new ArrayList<>(count);
        for (int ii = 0; ii < count; ++ii) {
            rowSets.add(builders[ii].build());
        }
        return rowSets;
    }

    /**
     * Build {@code rowSets} through a batcher at {@code batchSize} and assert that the result is their union and
     * nothing else, and that the inputs are left as they were found.
     */
    private static void checkBuild(final List<RowSet> rowSets, final long batchSize) {
        final long[] sizesBefore = rowSets.stream().mapToLong(RowSet::size).toArray();
        try (final WritableRowSet expected = reference(rowSets)) {
            final WritableRowSet actual;
            try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(batchSize)) {
                rowSets.forEach(rowSet -> batcher.add(rowSet.copy()));
                actual = batcher.build();
                assertThat(batcher.pendingBatchSize()).isZero();
            }
            try (actual) {
                assertThat(actual).isEqualTo(expected);
            }
        }
        assertThat(rowSets.stream().mapToLong(RowSet::size).toArray()).isEqualTo(sizesBefore);
    }

    @Test
    public void buildsTheUnionAtEveryBatchSize() {
        for (final int batchSize : new int[] {1, 2, 3, 7, 16, 1024}) {
            final List<RowSet> ascending = ascendingDisjoint(16);
            final List<RowSet> mixed = interleaved(16, 5);
            try {
                checkBuild(ascending, batchSize);
                checkBuild(mixed, batchSize);

                // The same blocks presented backwards: nothing appends, so every batch has to merge.
                final List<RowSet> descending = new ArrayList<>(ascending);
                Collections.reverse(descending);
                checkBuild(descending, batchSize);
            } finally {
                SafeCloseable.closeAll(ascending);
                SafeCloseable.closeAll(mixed);
            }
        }
    }

    @Test
    public void buildWithNothingAddedIsEmpty() {
        try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(RowSetUnionBatcher.maxBatchSize);
                final WritableRowSet built = batcher.build()) {
            assertThat(built.isEmpty()).isTrue();
        }
    }

    @Test
    public void ascendingInputCollapsesToOneRowSet() {
        final List<RowSet> rowSets = ascendingDisjoint(64);
        try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(8)) {
            for (final RowSet rowSet : rowSets) {
                batcher.add(rowSet.copy());
                // Each one appends to the run, so the batch never reaches a second entry and never fills.
                assertThat(batcher.pendingBatchSize()).isEqualTo(1);
            }
            try (final WritableRowSet built = batcher.build()) {
                assertThat(built.size()).isEqualTo(64 * 50);
            }
        } finally {
            SafeCloseable.closeAll(rowSets);
        }
    }

    /** The entries a row set stores, which is what the batcher weighs a prepend against. */
    private static long entryCount(final RowSet rowSet) {
        return ((WritableRowSetImpl) rowSet).getInnerSet().ixEntryCount();
    }

    @Test
    public void descendingInputOfGrowingRowSetsCollapsesToOneRowSet() {
        // Each row set is wholly below the run and holds at least as many entries as the run does, so folding it in
        // pays for moving the run with the entries it brings, and the batch never reaches a second slot.
        final List<RowSet> rowSets = new ArrayList<>();
        long below = 1_000_000;
        for (int entries = 1; entries <= 128; entries *= 2) {
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            below -= 2L * entries;
            for (int jj = 0; jj < entries; ++jj) {
                builder.appendKey(below + 2L * jj);
            }
            rowSets.add(builder.build());
        }
        try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(4)) {
            for (final RowSet rowSet : rowSets) {
                batcher.add(rowSet.copy());
                assertThat(batcher.pendingBatchSize()).isEqualTo(1);
            }
            try (final WritableRowSet built = batcher.build();
                    final WritableRowSet expected = reference(rowSets)) {
                assertThat(built).isEqualTo(expected);
            }
        } finally {
            SafeCloseable.closeAll(rowSets);
        }
    }

    @Test
    public void aPrependIsTakenOnlyWhenTheRunCanPayForIt() {
        // The guard is about relative size, not about prepends. A small row set below a large run would move the
        // whole run to save one slot, so it starts a run of its own and reaches the batch merge; the next one, below
        // a run its own size, folds in.
        final RowSetBuilderSequential bulk = RowSetFactory.builderSequential();
        for (int jj = 0; jj < 512; ++jj) {
            bulk.appendKey(1_000_000L + 2L * jj);
        }
        final List<RowSet> rowSets =
                List.of(bulk.build(), RowSetFactory.fromKeys(20, 21), RowSetFactory.fromKeys(16, 17));
        try {
            // The premise: the run really does store an entry per key, so moving it is what the guard weighs.
            assertThat(entryCount(rowSets.get(0))).isEqualTo(512);
            assertThat(entryCount(rowSets.get(1))).isEqualTo(1);
            try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(64)) {
                batcher.add(rowSets.get(0).copy());
                assertThat(batcher.pendingBatchSize()).isEqualTo(1);
                // Below the run, but 512 entries would move to take 1: a slot of its own.
                batcher.add(rowSets.get(1).copy());
                assertThat(batcher.pendingBatchSize()).isEqualTo(2);
                // Below that run, and no larger than it: folded in, so no new slot.
                batcher.add(rowSets.get(2).copy());
                assertThat(batcher.pendingBatchSize()).isEqualTo(2);
                try (final WritableRowSet built = batcher.build();
                        final WritableRowSet expected = reference(rowSets)) {
                    assertThat(built).isEqualTo(expected);
                }
            }
        } finally {
            SafeCloseable.closeAll(rowSets);
        }
    }

    @Test
    public void nonAppendingInputFillsTheBatch() {
        final List<RowSet> rowSets = interleaved(8, 4);
        try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(4)) {
            for (int ii = 0; ii < rowSets.size(); ++ii) {
                batcher.add(rowSets.get(ii).copy());
                // Nothing appends, so every row set takes a slot of its own and every fourth one empties the batch.
                assertThat(batcher.pendingBatchSize()).isEqualTo((ii + 1) % 4);
            }
            try (final WritableRowSet built = batcher.build()) {
                assertThat(built.size()).isEqualTo(8 * 4 * 5);
            }
        } finally {
            SafeCloseable.closeAll(rowSets);
        }
    }

    @Test
    public void anUnboundedRequestIsClampedToTheMaximum() {
        // A caller whose count is a table size can hand it straight over; what the count buys is merging once when it
        // is small, not an unbounded batch when it is not. Two ranges each, so the sets genuinely interleave and none
        // of them appends to the one before it.
        final List<RowSet> rowSets = interleaved(RowSetUnionBatcher.maxBatchSize + 1, 2);
        // A row count, which is what a caller with a table rather than a collection has to offer.
        try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(Long.MAX_VALUE)) {
            for (int ii = 0; ii < rowSets.size(); ++ii) {
                batcher.add(rowSets.get(ii).copy());
                assertThat(batcher.pendingBatchSize()).isLessThanOrEqualTo(RowSetUnionBatcher.maxBatchSize);
            }
            // The row set after the maximum found the batch already collapsed rather than still gathering.
            assertThat(batcher.groupCount()).isEqualTo(1);
            assertThat(batcher.pendingBatchSize()).isEqualTo(1);
            try (final WritableRowSet built = batcher.build();
                    final WritableRowSet expected = reference(rowSets)) {
                assertThat(built).isEqualTo(expected);
            }
        } finally {
            SafeCloseable.closeAll(rowSets);
        }
    }

    @Test
    public void aRequestBelowOneIsTreatedAsOne() {
        final List<RowSet> rowSets = interleaved(4, 2);
        for (final long batchSize : new long[] {Long.MIN_VALUE, Integer.MIN_VALUE, -1, 0}) {
            checkBuild(rowSets, batchSize);
        }
        SafeCloseable.closeAll(rowSets);
    }

    @Test
    public void collapsedBatchesFillTheGroupsBeforeFoldingTogether() {
        // Four row sets to a batch, and a fold only once a fifth group would be needed, so 20 row sets take the list
        // through one full cycle. Interleaved, so nothing appends and every one of them takes a slot.
        final int batchSize = 4;
        final List<RowSet> rowSets = interleaved(20, 3);
        try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(batchSize)) {
            for (int ii = 0; ii < rowSets.size(); ++ii) {
                batcher.add(rowSets.get(ii).copy());
                final int added = ii + 1;
                assertThat(batcher.pendingBatchSize()).isEqualTo(added % batchSize);
                // Whatever the state, the two regions fit in the list.
                assertThat(batcher.groupCount() + batcher.pendingBatchSize())
                        .isLessThanOrEqualTo(2 * batchSize);

                if (added == 4) {
                    // The first full batch collapsed into a group of its own rather than into a running result.
                    assertThat(batcher.groupCount()).isEqualTo(1);
                } else if (added == 16) {
                    // Four batches in, four groups, and nothing has been merged into twice.
                    assertThat(batcher.groupCount()).isEqualTo(4);
                } else if (added == 19) {
                    // The widest the list ever gets: every group slot taken and the batch one short of full.
                    assertThat(batcher.groupCount()).isEqualTo(4);
                    assertThat(batcher.pendingBatchSize()).isEqualTo(3);
                } else if (added == 20) {
                    // The batch that would have needed a fifth group folded all of them into one instead.
                    assertThat(batcher.groupCount()).isEqualTo(1);
                }
            }
            try (final WritableRowSet built = batcher.build();
                    final WritableRowSet expected = reference(rowSets)) {
                assertThat(built).isEqualTo(expected);
            }
        } finally {
            SafeCloseable.closeAll(rowSets);
        }
    }

    @Test
    public void aFullBatchLeavesTheGroupAvailableToAppendOnto() {
        // Two to a batch. The first two overlap, so neither falls clear of the other on either side and they fill the
        // batch; the third appends past the group they collapsed into, which only splices if the collapse left that
        // group as the run.
        try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(2)) {
            batcher.add(RowSetFactory.fromRange(100, 199));
            batcher.add(RowSetFactory.fromRange(0, 150));
            // The batch collapsed into one group covering [0, 199], which the next row set appends to.
            assertThat(batcher.groupCount()).isEqualTo(1);
            assertThat(batcher.pendingBatchSize()).isZero();
            batcher.add(RowSetFactory.fromRange(200, 299));
            assertThat(batcher.pendingBatchSize()).isZero();
            assertThat(batcher.groupCount()).isEqualTo(1);
            try (final WritableRowSet built = batcher.build()) {
                assertThat(built.size()).isEqualTo(300);
                assertThat(built.lastRowKey()).isEqualTo(299);
            }
        }
    }

    @Test
    public void emptyInputsAreDroppedAndClosed() {
        final WritableRowSet first = RowSetFactory.empty();
        final WritableRowSet second = RowSetFactory.empty();
        try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(2)) {
            batcher.add(first);
            batcher.add(second);
            // Neither one took a slot, so neither one can fill the batch or become a run to append to.
            assertThat(batcher.pendingBatchSize()).isZero();
            try (final WritableRowSet built = batcher.build()) {
                assertThat(built.isEmpty()).isTrue();
            }
        }
        // A row set handed over is closed on the way in whether it was kept or not, and a closed row set NPEs on use.
        assertThatThrownBy(first::size).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(second::size).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void aCopyHandedOverLeavesTheOriginalAloneAndIsReleasedAtTheEnd() {
        final List<RowSet> rowSets = interleaved(6, 4);
        try {
            final WritableRowSet built;
            try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(RowSetUnionBatcher.maxBatchSize)) {
                rowSets.forEach(rowSet -> batcher.add(rowSet.copy()));
                // Nothing appends here, so every input is held as a copy-on-write reference to it.
                assertThat(batcher.pendingBatchSize()).isEqualTo(6);
                for (final RowSet rowSet : rowSets) {
                    assertThat(refCount(rowSet)).isEqualTo(2);
                }
                built = batcher.build();
            }
            try (built) {
                // The merge consumed what it was handed, so nothing outlives the build to hold an input down.
                for (final RowSet rowSet : rowSets) {
                    assertThat(refCount(rowSet)).isEqualTo(1);
                    assertThat(rowSet.size()).isEqualTo(4 * 5);
                }
                assertThat(built.size()).isEqualTo(6 * 4 * 5);
            }
        } finally {
            SafeCloseable.closeAll(rowSets);
        }
    }

    @Test
    public void closeWithoutBuildReleasesEverything() {
        final List<RowSet> rowSets = interleaved(6, 4);
        final List<RowSet> more = interleaved(5, 4);
        try {
            try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(6)) {
                // The first six fill the batch and become the accumulated union; the rest are still outstanding.
                rowSets.forEach(rowSet -> batcher.add(rowSet.copy()));
                assertThat(batcher.pendingBatchSize()).isZero();
                more.forEach(rowSet -> batcher.add(rowSet.copy()));
                assertThat(batcher.pendingBatchSize()).isEqualTo(5);
            }
            // Both what was merged and what was not are released; nothing was handed anywhere.
            for (final RowSet rowSet : rowSets) {
                assertThat(refCount(rowSet)).isEqualTo(1);
            }
            for (final RowSet rowSet : more) {
                assertThat(refCount(rowSet)).isEqualTo(1);
            }
        } finally {
            SafeCloseable.closeAll(rowSets);
            SafeCloseable.closeAll(more);
        }
    }

    @Test
    public void buildLeavesTheBatcherReusable() {
        try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(RowSetUnionBatcher.maxBatchSize)) {
            batcher.add(RowSetFactory.fromRange(0, 9));
            try (final WritableRowSet first = batcher.build();
                    final WritableRowSet expectedFirst = RowSetFactory.fromRange(0, 9)) {
                assertThat(first).isEqualTo(expectedFirst);
            }
            // A build takes everything with it: the next row set is the whole of the next result.
            batcher.add(RowSetFactory.fromRange(20, 29));
            assertThat(batcher.pendingBatchSize()).isEqualTo(1);
            try (final WritableRowSet second = batcher.build();
                    final WritableRowSet expectedSecond = RowSetFactory.fromRange(20, 29)) {
                assertThat(second).isEqualTo(expectedSecond);
            }
            try (final WritableRowSet third = batcher.build()) {
                assertThat(third.isEmpty()).isTrue();
            }
        }
    }

    @Test
    public void mergedBatchesAccumulateAcrossFlushes() {
        final List<RowSet> rowSets = interleaved(9, 4);
        try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(2)) {
            // Four merges of two plus one left over, so the result has to survive being merged into repeatedly.
            rowSets.forEach(rowSet -> batcher.add(rowSet.copy()));
            try (final WritableRowSet built = batcher.build();
                    final WritableRowSet expected = reference(rowSets)) {
                assertThat(built).isEqualTo(expected);
            }
        } finally {
            SafeCloseable.closeAll(rowSets);
        }
    }

    @Test
    public void buildsTheUnionOverRandomInput() {
        final Random random = new Random(0xB47C4E);
        for (int trial = 0; trial < 50; ++trial) {
            final int count = 1 + random.nextInt(40);
            final List<RowSet> rowSets = new ArrayList<>(count);
            for (int ii = 0; ii < count; ++ii) {
                final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
                // No ranges at all builds an empty row set, which is a shape the batcher has to drop.
                final int ranges = random.nextInt(5);
                for (int rr = 0; rr < ranges; ++rr) {
                    final long start = random.nextInt(5000);
                    builder.addRange(start, start + random.nextInt(20));
                }
                rowSets.add(builder.build());
            }
            try {
                checkBuild(rowSets, 1 + random.nextInt(8));
            } finally {
                SafeCloseable.closeAll(rowSets);
            }
        }
    }

    /**
     * Only meaningful for an implementation that shares under copy-on-write; the single range shapes above always
     * report one.
     */
    private static int refCount(final RowSet rowSet) {
        return ((WritableRowSetImpl) rowSet).getInnerSet().ixRefCount();
    }

    @Test
    public void configuredCapIsTakenAsIs() {
        final int saved = RowSetUnionBatcher.maxBatchSize;
        try {
            // Any configured cap is honoured, however large; a request under it is what sizes the batch.
            RowSetUnionBatcher.maxBatchSize = Integer.MAX_VALUE;
            try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(100)) {
                assertThat(batcher.batchSize()).isEqualTo(100);
            }
            // A batch the entries list could not hold, or an empty one, is refused rather than clamped.
            assertThatThrownBy(() -> new RowSetUnionBatcher(Long.MAX_VALUE).close())
                    .isInstanceOf(IllegalArgumentException.class);
            for (final int cap : new int[] {0, -5}) {
                RowSetUnionBatcher.maxBatchSize = cap;
                assertThatThrownBy(() -> new RowSetUnionBatcher(100).close())
                        .isInstanceOf(IllegalArgumentException.class);
            }
            // A request above the cap is held to it.
            RowSetUnionBatcher.maxBatchSize = 300;
            try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(Long.MAX_VALUE)) {
                assertThat(batcher.batchSize()).isEqualTo(300);
            }
            // With a cap of one every add merges, and the union is still right.
            RowSetUnionBatcher.maxBatchSize = 1;
            final List<RowSet> rowSets = interleaved(10, 2);
            try (final WritableRowSet expected = RowSetFactory.union(rowSets);
                    final RowSetUnionBatcher batcher = new RowSetUnionBatcher(rowSets.size())) {
                for (final RowSet rowSet : rowSets) {
                    batcher.add(rowSet.copy());
                }
                try (final WritableRowSet actual = batcher.build()) {
                    assertThat(actual).isEqualTo(expected);
                }
            } finally {
                for (final RowSet rowSet : rowSets) {
                    rowSet.close();
                }
            }
        } finally {
            RowSetUnionBatcher.maxBatchSize = saved;
        }
    }
}
