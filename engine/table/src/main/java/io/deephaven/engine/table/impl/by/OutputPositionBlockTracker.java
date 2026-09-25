//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.util.mutable.MutableLong;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Counts the live states in each block of aggregation output positions, so that a block whose positions have all been
 * assigned and whose states have all been removed can be released as a whole.
 *
 * <p>
 * Output positions are assigned in increasing order and never reused, and a state that is empty at the end of a cycle
 * is removed, so once every position in a block has been assigned and the block's live count reaches zero, nothing will
 * ever read or write the block again.
 * </p>
 */
final class OutputPositionBlockTracker {
    private static final int BLOCK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;
    private static final int LOG_BLOCK_SIZE = Integer.numberOfTrailingZeros(BLOCK_SIZE);
    private static final int RELEASED = -1;

    /** The number of live states in each block, or {@link #RELEASED}. */
    private int[] liveCounts = new int[0];
    /** Every position in the blocks below this one has been assigned. */
    private int closedBlocks;

    /** A closed block with at most this many live states is sparse, and may be collapsed. */
    private final int sparseLiveLimit;
    /** The closed blocks that hold live states, but no more than {@link #sparseLiveLimit}. */
    private final IntSortedSet sparseBlocks = new IntRBTreeSet();

    /**
     * @param initialStates the output positions of the live states after the initial build
     * @param nextOutputPosition the next output position that will be assigned
     * @param collapseFreeFraction a closed block at least this fraction free is sparse, and runs of adjacent sparse
     *        blocks are collapsed; 1 or more disables collapsing
     */
    OutputPositionBlockTracker(final RowSet initialStates, final int nextOutputPosition,
            final double collapseFreeFraction) {
        sparseLiveLimit = collapseFreeFraction >= 1 ? 0 : (int) (BLOCK_SIZE * (1 - collapseFreeFraction));
        ensureCapacity(nextOutputPosition);
        adjust(initialStates, 1);
        closedBlocks = nextOutputPosition >> LOG_BLOCK_SIZE;
        for (int bi = 0; bi < closedBlocks; ++bi) {
            updateSparse(bi);
        }
    }

    /**
     * Account for the states that became live or empty in an update cycle, and find the blocks that can be released.
     *
     * @param added the output positions of states that became live
     * @param removed the output positions of states that were removed because they were empty at the end of the cycle
     * @param nextOutputPosition the next output position that will be assigned
     * @return the output positions of the blocks to release, which the caller owns
     */
    WritableRowSet update(final RowSet added, final RowSet removed, final int nextOutputPosition) {
        ensureCapacity(nextOutputPosition);
        adjust(added, 1);
        adjust(removed, -1);

        // a block's live count can only reach zero through a removal, or be zero already when it closes
        final int newClosedBlocks = nextOutputPosition >> LOG_BLOCK_SIZE;
        final IntSortedSet candidates = new IntRBTreeSet();
        removed.forAllRowKeyRanges((first, last) -> {
            final int lastBlock = Math.min((int) (last >> LOG_BLOCK_SIZE), newClosedBlocks - 1);
            for (int bi = (int) (first >> LOG_BLOCK_SIZE); bi <= lastBlock; ++bi) {
                candidates.add(bi);
            }
        });
        for (int bi = closedBlocks; bi < newClosedBlocks; ++bi) {
            candidates.add(bi);
        }
        closedBlocks = newClosedBlocks;

        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        candidates.forEach((int bi) -> {
            if (liveCounts[bi] == 0) {
                release(bi, builder);
            } else {
                updateSparse(bi);
            }
        });
        return builder.build();
    }

    /**
     * Collapse runs of adjacent sparse blocks: within each run, shift the live states toward the run's first position,
     * preserving their order, so that the blocks at the end of the run are left empty and can be released. States
     * outside the runs do not move.
     *
     * @param liveStates the output positions of the live states, after this cycle's additions and removals
     * @param maxShiftedStates the most states to move in this cycle
     * @param released the output positions of blocks to release, to which the emptied blocks are added
     * @return the collapsed runs and the shifts that move their live states, which only move states toward lower
     *         positions
     */
    Collapse collapseSparseBlocks(final RowSet liveStates, final long maxShiftedStates,
            final WritableRowSet released) {
        if (sparseBlocks.size() < 2) {
            return Collapse.NONE;
        }
        final List<long[]> collapsedRuns = new ArrayList<>();
        final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
        final RowSetBuilderSequential releasedBuilder = RowSetFactory.builderSequential();
        long remainingShifts = maxShiftedStates;

        // find the runs of consecutive sparse blocks first, since collapsing a run changes the sparse set
        final List<int[]> runs = new ArrayList<>();
        int runFirst = -1;
        int runLast = -1;
        for (final IntIterator it = sparseBlocks.iterator(); it.hasNext();) {
            final int bi = it.nextInt();
            if (runFirst >= 0 && bi == runLast + 1) {
                runLast = bi;
                continue;
            }
            if (runFirst >= 0) {
                runs.add(new int[] {runFirst, runLast});
            }
            runFirst = runLast = bi;
        }
        runs.add(new int[] {runFirst, runLast});

        for (final int[] run : runs) {
            // take as long a prefix of the run as the shift budget allows
            long runLive = 0;
            int collapseLast = run[0] - 1;
            for (int bi = run[0]; bi <= run[1] && runLive + liveCounts[bi] <= remainingShifts; ++bi) {
                runLive += liveCounts[bi];
                collapseLast = bi;
            }
            final int blocks = collapseLast - run[0] + 1;
            final int blocksAfter = (int) ((runLive + BLOCK_SIZE - 1) >> LOG_BLOCK_SIZE);
            if (blocks < 2 || blocksAfter >= blocks) {
                continue;
            }
            remainingShifts -= runLive;

            final long firstPosition = (long) run[0] << LOG_BLOCK_SIZE;
            final long lastPosition = ((long) (collapseLast + 1) << LOG_BLOCK_SIZE) - 1;
            collapsedRuns.add(new long[] {firstPosition, lastPosition, runLive});
            try (final RowSet runStates = liveStates.subSetByKeyRange(firstPosition, lastPosition)) {
                final MutableLong destination = new MutableLong(firstPosition);
                runStates.forAllRowKeyRanges((first, last) -> {
                    if (first != destination.get()) {
                        shiftBuilder.shiftRange(first, last, destination.get() - first);
                    }
                    destination.add(last - first + 1);
                });
            }

            for (int bi = run[0]; bi <= collapseLast; ++bi) {
                sparseBlocks.remove(bi);
                final long blockLive =
                        Math.max(0, Math.min(BLOCK_SIZE, runLive - ((long) (bi - run[0]) << LOG_BLOCK_SIZE)));
                liveCounts[bi] = (int) blockLive;
                if (blockLive == 0) {
                    release(bi, releasedBuilder);
                } else {
                    updateSparse(bi);
                }
            }
            if (remainingShifts <= 0) {
                break;
            }
        }

        try (final RowSet newlyReleased = releasedBuilder.build()) {
            released.insert(newlyReleased);
        }
        return new Collapse(shiftBuilder.build(), collapsedRuns);
    }

    /**
     * The runs collapsed in one cycle. Within a run, the live states keep their order and are moved to occupy the run's
     * first positions.
     */
    static final class Collapse {
        static final Collapse NONE = new Collapse(RowSetShiftData.EMPTY, List.of());

        /** The shifts that move the live states; nonempty exactly when some run was collapsed. */
        final RowSetShiftData shift;
        /** For each collapsed run, in order: its first position, its last position, and its number of live states. */
        private final List<long[]> runs;

        private Collapse(final RowSetShiftData shift, final List<long[]> runs) {
            this.shift = shift;
            this.runs = runs;
        }

        /**
         * Apply the collapse to the live states and to row sets of states within them. Each run of the live states is
         * replaced by one contiguous range, which is far cheaper than applying {@link #shift} range by range when the
         * live states are scattered.
         *
         * @param liveStates the output positions of the live states, before the collapse
         * @param subsets row sets of positions that are all in {@code liveStates}, such as the states added or modified
         *        this cycle
         */
        void apply(final WritableRowSet liveStates, final WritableRowSet... subsets) {
            for (final long[] run : runs) {
                final long first = run[0];
                final long last = run[1];
                // a state's new position is the run's first position plus its rank among the run's live states
                try (final RowSet runStates = liveStates.subSetByKeyRange(first, last)) {
                    for (final WritableRowSet subset : subsets) {
                        try (final RowSet moving = subset.subSetByKeyRange(first, last)) {
                            if (moving.isEmpty()) {
                                continue;
                            }
                            final RowSetBuilderSequential moved = RowSetFactory.builderSequential();
                            moving.forAllRowKeys(key -> moved.appendKey(first + runStates.find(key)));
                            subset.removeRange(first, last);
                            try (final RowSet movedKeys = moved.build()) {
                                subset.insert(movedKeys);
                            }
                        }
                    }
                }
                liveStates.removeRange(first, last);
                if (run[2] > 0) {
                    liveStates.insertRange(first, first + run[2] - 1);
                }
            }
        }
    }

    private void release(final int bi, final RowSetBuilderSequential builder) {
        liveCounts[bi] = RELEASED;
        sparseBlocks.remove(bi);
        final long first = (long) bi << LOG_BLOCK_SIZE;
        builder.appendRange(first, first + BLOCK_SIZE - 1);
    }

    private void updateSparse(final int bi) {
        if (liveCounts[bi] > 0 && liveCounts[bi] <= sparseLiveLimit) {
            sparseBlocks.add(bi);
        } else {
            sparseBlocks.remove(bi);
        }
    }

    private void ensureCapacity(final int nextOutputPosition) {
        final int blocksNeeded = (nextOutputPosition + BLOCK_SIZE - 1) >> LOG_BLOCK_SIZE;
        if (blocksNeeded > liveCounts.length) {
            liveCounts = Arrays.copyOf(liveCounts, Math.max(blocksNeeded, liveCounts.length * 2));
        }
    }

    private void adjust(final RowSet positions, final int delta) {
        positions.forAllRowKeyRanges((first, last) -> {
            long rangeFirst = first;
            while (rangeFirst <= last) {
                final int bi = (int) (rangeFirst >> LOG_BLOCK_SIZE);
                final long blockLast = ((long) bi << LOG_BLOCK_SIZE) + BLOCK_SIZE - 1;
                final long rangeLast = Math.min(last, blockLast);
                Assert.neq(liveCounts[bi], "liveCounts[bi]", RELEASED, "RELEASED");
                liveCounts[bi] += delta * (int) (rangeLast - rangeFirst + 1);
                Assert.geqZero(liveCounts[bi], "liveCounts[bi]");
                rangeFirst = rangeLast + 1;
            }
        });
    }
}
