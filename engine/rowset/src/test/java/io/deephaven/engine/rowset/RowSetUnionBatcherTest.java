//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
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

    private static void closeAll(final List<RowSet> rowSets) {
        rowSets.forEach(RowSet::close);
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
    private static void checkBuild(final List<RowSet> rowSets, final int batchSize) {
        final long[] sizesBefore = rowSets.stream().mapToLong(RowSet::size).toArray();
        try (final WritableRowSet expected = reference(rowSets)) {
            final WritableRowSet actual;
            try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(batchSize)) {
                rowSets.forEach(batcher::addCopy);
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
                closeAll(ascending);
                closeAll(mixed);
            }
        }
    }

    @Test
    public void buildWithNothingAddedIsEmpty() {
        try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(RowSetUnionBatcher.DEFAULT_BATCH_SIZE);
                final WritableRowSet built = batcher.build()) {
            assertThat(built.isEmpty()).isTrue();
        }
    }

    @Test
    public void ascendingInputCollapsesToOneRowSet() {
        final List<RowSet> rowSets = ascendingDisjoint(64);
        try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(8)) {
            for (final RowSet rowSet : rowSets) {
                batcher.addCopy(rowSet);
                // Each one appends to the run, so the batch never reaches a second entry and never fills.
                assertThat(batcher.pendingBatchSize()).isEqualTo(1);
            }
            try (final WritableRowSet built = batcher.build()) {
                assertThat(built.size()).isEqualTo(64 * 50);
            }
        } finally {
            closeAll(rowSets);
        }
    }

    @Test
    public void nonAppendingInputFillsTheBatch() {
        final List<RowSet> rowSets = interleaved(8, 4);
        try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(4)) {
            for (int ii = 0; ii < rowSets.size(); ++ii) {
                batcher.addCopy(rowSets.get(ii));
                // Nothing appends, so every row set takes a slot of its own and every fourth one empties the batch.
                assertThat(batcher.pendingBatchSize()).isEqualTo((ii + 1) % 4);
            }
            try (final WritableRowSet built = batcher.build()) {
                assertThat(built.size()).isEqualTo(8 * 4 * 5);
            }
        } finally {
            closeAll(rowSets);
        }
    }

    @Test
    public void emptyInputsAreDroppedAndTheOwnedOneIsClosed() {
        final WritableRowSet owned = RowSetFactory.empty();
        try (final WritableRowSet borrowed = RowSetFactory.empty();
                final RowSetUnionBatcher batcher = new RowSetUnionBatcher(2)) {
            batcher.add(owned);
            batcher.addCopy(borrowed);
            // Neither one took a slot, so neither one can fill the batch or become a run to append to.
            assertThat(batcher.pendingBatchSize()).isZero();
            try (final WritableRowSet built = batcher.build()) {
                assertThat(built.isEmpty()).isTrue();
            }
        }
        // A row set handed over is closed on the way in whether it was kept or not, and a closed row set NPEs on use.
        assertThatThrownBy(owned::size).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void addCopyLeavesTheInputOpenAndReleasesItAtTheEnd() {
        final List<RowSet> rowSets = interleaved(6, 4);
        try {
            final WritableRowSet built;
            try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(RowSetUnionBatcher.DEFAULT_BATCH_SIZE)) {
                rowSets.forEach(batcher::addCopy);
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
            closeAll(rowSets);
        }
    }

    @Test
    public void closeWithoutBuildReleasesEverything() {
        final List<RowSet> rowSets = interleaved(6, 4);
        final List<RowSet> more = interleaved(5, 4);
        try {
            try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(6)) {
                // The first six fill the batch and become the accumulated union; the rest are still outstanding.
                rowSets.forEach(batcher::addCopy);
                assertThat(batcher.pendingBatchSize()).isZero();
                more.forEach(batcher::addCopy);
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
            closeAll(rowSets);
            closeAll(more);
        }
    }

    @Test
    public void buildLeavesTheBatcherReusable() {
        try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(RowSetUnionBatcher.DEFAULT_BATCH_SIZE)) {
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
            rowSets.forEach(batcher::addCopy);
            try (final WritableRowSet built = batcher.build();
                    final WritableRowSet expected = reference(rowSets)) {
                assertThat(built).isEqualTo(expected);
            }
        } finally {
            closeAll(rowSets);
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
                closeAll(rowSets);
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
}
