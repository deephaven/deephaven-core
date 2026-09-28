//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestOutputPositionBlockTracker {
    private static final int BLOCK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    /**
     * Applying a collapse to the live states and to row sets within them matches applying its shift, with many runs,
     * released blocks within them, and budgets that cut runs short.
     */
    @Test
    public void testApplyMatchesShift() {
        final int blocks = 64;
        final int size = blocks * BLOCK_SIZE;
        for (int seed = 0; seed < 20; ++seed) {
            final Random random = new Random(seed);
            final double collapseFreeFraction = 0.2 + 0.7 * random.nextDouble();
            final OutputPositionBlockTracker tracker = new OutputPositionBlockTracker(size, collapseFreeFraction);

            // Empty some blocks entirely, leave a few full, and leave the rest sparse, so that runs are long and often
            // have released blocks within them.
            final double sparseKeep = 1 - collapseFreeFraction;
            final RowSetBuilderSequential removedBuilder = RowSetFactory.builderSequential();
            for (int bi = 0; bi < blocks; ++bi) {
                final int kind = random.nextInt(16);
                final double keep = kind < 5 ? 0 : kind == 5 ? 1 : sparseKeep * random.nextDouble();
                for (int ii = 0; ii < BLOCK_SIZE; ++ii) {
                    if (random.nextDouble() >= keep) {
                        removedBuilder.appendKey((long) bi * BLOCK_SIZE + ii);
                    }
                }
            }
            try (final WritableRowSet removed = removedBuilder.build();
                    final WritableRowSet liveStates = RowSetFactory.flat(size);
                    final WritableRowSet noneAdded = RowSetFactory.empty()) {
                liveStates.remove(removed);
                final WritableRowSet released = tracker.update(noneAdded, removed, size);
                final long budget = random.nextBoolean() ? Long.MAX_VALUE : random.nextInt(liveStates.intSize() + 1);
                final OutputPositionBlockTracker.Collapse collapse =
                        tracker.collapseSparseBlocks(liveStates, budget, released);
                released.close();
                if (collapse.shift.empty()) {
                    continue;
                }

                try (final WritableRowSet scattered = randomSubset(random, liveStates, random.nextDouble());
                        final WritableRowSet dense = liveStates.subSetByPositionRange(
                                random.nextInt(liveStates.intSize()), liveStates.size());
                        final WritableRowSet expectedLive = liveStates.copy();
                        final WritableRowSet expectedScattered = scattered.copy();
                        final WritableRowSet expectedDense = dense.copy()) {
                    collapse.shift.apply(expectedLive);
                    collapse.shift.apply(expectedScattered);
                    collapse.shift.apply(expectedDense);

                    collapse.apply(liveStates, scattered, dense);
                    assertEquals("seed " + seed, expectedLive, liveStates);
                    assertEquals("seed " + seed, expectedScattered, scattered);
                    assertEquals("seed " + seed, expectedDense, dense);
                    assertTrue("seed " + seed, scattered.subsetOf(liveStates) && dense.subsetOf(liveStates));
                }
            }
        }
    }

    /**
     * Two adjacent sparse blocks whose states need both blocks are not collapsed: moving them would release nothing.
     */
    @Test
    public void testRunThatWouldReleaseNothingIsLeft() {
        final int size = 4 * BLOCK_SIZE;
        final OutputPositionBlockTracker tracker = new OutputPositionBlockTracker(size, 0.25);
        final int keep = BLOCK_SIZE * 3 / 4 - 36;
        try (final WritableRowSet removed = RowSetFactory.fromRange(keep, BLOCK_SIZE - 1);
                final WritableRowSet noneAdded = RowSetFactory.empty();
                final WritableRowSet liveStates = RowSetFactory.flat(size)) {
            removed.insertRange(BLOCK_SIZE + keep, 2L * BLOCK_SIZE - 1);
            liveStates.remove(removed);
            try (final WritableRowSet released = tracker.update(noneAdded, removed, size)) {
                assertTrue(released.isEmpty());
                final OutputPositionBlockTracker.Collapse collapse =
                        tracker.collapseSparseBlocks(liveStates, Long.MAX_VALUE, released);
                assertTrue(collapse.shift.empty());
                assertTrue(released.isEmpty());
            }
        }
    }

    private static WritableRowSet randomSubset(final Random random, final RowSet rowSet, final double fraction) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        rowSet.forAllRowKeys(key -> {
            if (random.nextDouble() < fraction) {
                builder.appendKey(key);
            }
        });
        return builder.build();
    }
}
