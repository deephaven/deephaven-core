//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.mutable.MutableLong;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 * Counts the live states in each block of aggregation output positions, so that a block whose positions have all been
 * assigned and whose states have all been removed can be released as a whole.
 *
 * <p>
 * Output positions are assigned in increasing order, and a state that is empty at the end of a cycle is removed, so
 * once every position in a block has been assigned and the block's live count reaches zero, no state will be assigned
 * to the block again. New states are only ever assigned after every existing one. Released blocks can still be
 * reclaimed by moves that keep the states in order: collapsing runs of sparse blocks, and shifting the blocks after
 * released ones down over them.
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
    /** The blocks whose live count is {@link #RELEASED}. */
    private final BitSet releasedBlocks = new BitSet();
    /** The number of blocks in {@link #releasedBlocks}. */
    private int releasedBlockCount;
    /**
     * The budget a block shift left unspent because the next block had more live states than it could move, carried to
     * the next cycle so that small cycles still make progress; at most a block's worth.
     */
    private long carriedShiftBudget;
    /**
     * The first of the released blocks a block shift left in the middle, from which the next one resumes; negative when
     * the last block shift reached the end. Resuming rather than starting over from the first released block keeps a
     * sweep moving toward the end: blocks released before it, often those it just moved, wait for the next sweep.
     */
    private int sweepBlock = -1;

    /** A closed block with at most this many live states is sparse, and may be collapsed. */
    private final int sparseLiveLimit;
    /** The closed blocks that hold live states, but no more than {@link #sparseLiveLimit}. */
    private final BitSet sparseBlocks = new BitSet();
    /** The number of blocks in {@link #sparseBlocks}. */
    private int sparseBlockCount;

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

        // A block's live count can only reach zero through a removal, or be zero already when it closes. The removed
        // positions are visited in increasing order, so each block they touch is finished once the walk moves past
        // it; blocks that closed this cycle are finished afterward, which keeps the released blocks in order.
        final int oldClosedBlocks = closedBlocks;
        final int newClosedBlocks = nextOutputPosition >> LOG_BLOCK_SIZE;
        closedBlocks = newClosedBlocks;
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        final MutableInt currentBlock = new MutableInt(-1);
        removed.forAllRowKeyRanges((first, last) -> {
            long rangeFirst = first;
            while (rangeFirst <= last) {
                final int bi = (int) (rangeFirst >> LOG_BLOCK_SIZE);
                final long rangeLast = Math.min(last, ((long) bi << LOG_BLOCK_SIZE) + BLOCK_SIZE - 1);
                if (bi != currentBlock.get()) {
                    finishRemovedBlock(currentBlock.get(), oldClosedBlocks, builder);
                    currentBlock.set(bi);
                }
                assert liveCounts[bi] != RELEASED;
                liveCounts[bi] -= (int) (rangeLast - rangeFirst + 1);
                assert liveCounts[bi] >= 0;
                rangeFirst = rangeLast + 1;
            }
        });
        finishRemovedBlock(currentBlock.get(), oldClosedBlocks, builder);
        for (int bi = oldClosedBlocks; bi < newClosedBlocks; ++bi) {
            finishClosedBlock(bi, builder);
        }
        return builder.build();
    }

    /**
     * Finish a block whose states were removed this cycle, unless it closed this cycle, in which case it is finished
     * with the other newly closed blocks.
     */
    private void finishRemovedBlock(final int bi, final int oldClosedBlocks, final RowSetBuilderSequential builder) {
        if (bi >= 0 && bi < oldClosedBlocks) {
            finishClosedBlock(bi, builder);
        }
    }

    private void finishClosedBlock(final int bi, final RowSetBuilderSequential builder) {
        if (liveCounts[bi] == 0) {
            release(bi, builder);
        } else {
            updateSparse(bi);
        }
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
        if (sparseBlockCount < 2) {
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
        for (int bi = sparseBlocks.nextSetBit(0); bi >= 0; bi = sparseBlocks.nextSetBit(bi + 1)) {
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
                clearSparse(bi);
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
         * @return the live states in the collapsed runs, which bounds the states the collapse moves
         */
        long movedStates() {
            long moved = 0;
            for (final long[] run : runs) {
                moved += run[2];
            }
            return moved;
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

    /**
     * Plan to shift blocks down over the released blocks, keeping the states in order, once the released blocks are at
     * least {@code blockShiftFraction} of the positions assigned. Starting at the released blocks the last shift left,
     * or else at the first released block, each block of live states after it moves down by the number of released
     * blocks passed over, as whole blocks, until the budget of live states to move runs out. The released blocks passed
     * over become one run of released blocks just before the first block not moved, from which the next cycle resumes;
     * if every block moves, they are given back at the end instead, and the next output position moves down by them.
     *
     * @param blockShiftFraction the fraction of the positions assigned that the released blocks must reach; zero shifts
     *        for any released block, and a negative fraction never shifts
     * @param nextOutputPosition the next output position that will be assigned
     * @param budget the most live states to move in this cycle, before any carried from the last one
     * @return the plan, which {@link #applyBlockShift} records once the moves are made
     */
    BlockShift planBlockShift(final double blockShiftFraction, final int nextOutputPosition, final long budget) {
        if (blockShiftFraction < 0 || releasedBlockCount == 0
                || ((long) releasedBlockCount << LOG_BLOCK_SIZE) < blockShiftFraction * nextOutputPosition) {
            carriedShiftBudget = 0;
            return BlockShift.NONE;
        }
        final long available = Math.max(0, budget) + carriedShiftBudget;
        final int blocksInUse = blocksInUse(nextOutputPosition);
        final int firstReleased = sweepBlock >= 0 ? sweepBlock : releasedBlocks.nextSetBit(0);
        assert liveCounts[firstReleased] == RELEASED;
        final List<int[]> ranges = new ArrayList<>();
        int gap = 0;
        long moved = 0;
        int stop = blocksInUse;
        for (int bi = firstReleased; bi < blocksInUse;) {
            if (liveCounts[bi] == RELEASED) {
                final int releasedEnd = Math.min(releasedBlocks.nextClearBit(bi), blocksInUse);
                gap += releasedEnd - bi;
                bi = releasedEnd;
                continue;
            }
            if (moved + liveCounts[bi] > available) {
                stop = bi;
                break;
            }
            moved += liveCounts[bi];
            addBlockToRanges(ranges, bi, gap);
            ++bi;
        }
        final boolean reachedEnd = stop == blocksInUse;
        carriedShiftBudget = reachedEnd ? 0 : Math.min(BLOCK_SIZE, available - moved);
        if (ranges.isEmpty()) {
            return BlockShift.NONE;
        }
        return new BlockShift(ranges, firstReleased, stop, gap, reachedEnd, moved);
    }

    private static void addBlockToRanges(final List<int[]> ranges, final int bi, final int gap) {
        final int[] last = ranges.isEmpty() ? null : ranges.get(ranges.size() - 1);
        if (last != null && last[1] == bi - 1 && last[2] == gap) {
            last[1] = bi;
        } else {
            ranges.add(new int[] {bi, bi, gap});
        }
    }

    /**
     * Account for the moves of a planned block shift.
     *
     * @param plan the plan returned by {@link #planBlockShift}, whose moves have been made
     */
    void applyBlockShift(final BlockShift plan) {
        for (final int[] range : plan.blockRanges) {
            for (int bi = range[0]; bi <= range[1]; ++bi) {
                final int destination = bi - range[2];
                liveCounts[destination] = liveCounts[bi];
                if (sparseBlocks.get(bi)) {
                    sparseBlocks.clear(bi);
                    sparseBlocks.set(destination);
                }
            }
        }
        releasedBlocks.clear(plan.firstReleasedBlock, plan.stopBlock);
        if (plan.reachedEnd) {
            // the blocks given back are unassigned again, like every block past those in use
            Arrays.fill(liveCounts, plan.stopBlock - plan.gapBlocks, plan.stopBlock, 0);
            releasedBlockCount -= plan.gapBlocks;
            closedBlocks -= plan.gapBlocks;
            sweepBlock = -1;
        } else {
            Arrays.fill(liveCounts, plan.stopBlock - plan.gapBlocks, plan.stopBlock, RELEASED);
            releasedBlocks.set(plan.stopBlock - plan.gapBlocks, plan.stopBlock);
            sweepBlock = plan.stopBlock - plan.gapBlocks;
        }
    }

    /**
     * A shift of whole blocks down over released blocks, keeping the states in order.
     */
    static final class BlockShift {
        static final BlockShift NONE = new BlockShift(List.of(), 0, 0, 0, false, 0);

        /** The moves, as whole blocks; empty for no shift. */
        final RowSetShiftData shift;
        /** For each run of blocks moved by the same amount: its first block, its last block, and the blocks moved. */
        private final List<int[]> blockRanges;
        private final int firstReleasedBlock;
        /** The first block not moved, or the number of blocks in use if every block after a released one moved. */
        private final int stopBlock;
        /** The released blocks passed over, which the blocks after them moved down by. */
        private final int gapBlocks;
        /** Whether every block after the first released one moved, giving the released blocks back at the end. */
        private final boolean reachedEnd;
        /** The live states moved. */
        final long movedStates;

        private BlockShift(final List<int[]> blockRanges, final int firstReleasedBlock, final int stopBlock,
                final int gapBlocks, final boolean reachedEnd, final long movedStates) {
            this.blockRanges = blockRanges;
            this.firstReleasedBlock = firstReleasedBlock;
            this.stopBlock = stopBlock;
            this.gapBlocks = gapBlocks;
            this.reachedEnd = reachedEnd;
            this.movedStates = movedStates;
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            for (final int[] range : blockRanges) {
                builder.shiftRange((long) range[0] << LOG_BLOCK_SIZE, ((long) (range[1] + 1) << LOG_BLOCK_SIZE) - 1,
                        -((long) range[2] << LOG_BLOCK_SIZE));
            }
            shift = builder.build();
        }

        /**
         * @return the positions given back at the end, by which the next output position moves down
         */
        long reclaimedPositions() {
            return reachedEnd ? (long) gapBlocks << LOG_BLOCK_SIZE : 0;
        }

        /**
         * @return the positions, after the shift, whose storage may be released: the released blocks left before the
         *         first block not moved, or, if they were given back at the end, every position from the new end on, so
         *         that the storage is allocated again when states are assigned there
         */
        RowSet remainingReleased() {
            if (gapBlocks == 0) {
                return RowSetFactory.empty();
            }
            final long first = (long) (stopBlock - gapBlocks) << LOG_BLOCK_SIZE;
            if (reachedEnd) {
                // output positions are ints, so this reaches every position that may have storage
                return RowSetFactory.fromRange(first, Integer.MAX_VALUE);
            }
            return RowSetFactory.fromRange(first, ((long) stopBlock << LOG_BLOCK_SIZE) - 1);
        }

        /**
         * @return the first position, before the shift, that this shift leaves in place
         */
        long firstUnmovedPosition() {
            return (long) stopBlock << LOG_BLOCK_SIZE;
        }

        /** The amount this shift moves {@code position} by, zero if it does not move it. */
        private long deltaAt(final long position) {
            final int bi = (int) (position >> LOG_BLOCK_SIZE);
            int low = 0;
            int high = blockRanges.size() - 1;
            while (low <= high) {
                final int mid = (low + high) >>> 1;
                final int[] range = blockRanges.get(mid);
                if (range[1] < bi) {
                    low = mid + 1;
                } else if (range[0] > bi) {
                    high = mid - 1;
                } else {
                    return -((long) range[2] << LOG_BLOCK_SIZE);
                }
            }
            return 0;
        }

        /**
         * The shift that collapses {@code collapse}'s runs and then makes this shift's moves, as one shift from the
         * positions before either. Both keep the states in order, so their composition does too. A state in a collapsed
         * run moves by its collapse delta plus this shift's delta where the collapse put it; every other position this
         * shift moves keeps its delta. Within a run only the live states are named, since the positions they vacate
         * would otherwise land among them. This describes the moves to downstream listeners; the storage takes
         * {@code collapse.shift} and then {@link #shift}.
         *
         * @param collapse the collapse made in the same cycle, before this shift
         * @param liveStates the output positions of the live states, before the collapse
         */
        RowSetShiftData composedWith(final Collapse collapse, final RowSet liveStates) {
            if (collapse.runs.isEmpty()) {
                return shift;
            }
            final List<long[]> pieces = new ArrayList<>();
            // this shift's ranges, less the collapsed runs, which are named state by state below
            for (final int[] range : blockRanges) {
                long first = (long) range[0] << LOG_BLOCK_SIZE;
                final long last = ((long) (range[1] + 1) << LOG_BLOCK_SIZE) - 1;
                final long delta = -((long) range[2] << LOG_BLOCK_SIZE);
                for (final long[] run : collapse.runs) {
                    if (run[1] < first || run[0] > last) {
                        continue;
                    }
                    if (first < run[0]) {
                        pieces.add(new long[] {first, run[0] - 1, delta});
                    }
                    first = run[1] + 1;
                }
                if (first <= last) {
                    pieces.add(new long[] {first, last, delta});
                }
            }
            for (final long[] run : collapse.runs) {
                try (final RowSet runStates = liveStates.subSetByKeyRange(run[0], run[1])) {
                    final MutableLong destination = new MutableLong(run[0]);
                    runStates.forAllRowKeyRanges((rangeFirst, rangeLast) -> {
                        // split where this shift's delta at the destinations changes, which is only at a block
                        long sourceFirst = rangeFirst;
                        while (sourceFirst <= rangeLast) {
                            final long destinationFirst = destination.get();
                            final long blockEnd = (destinationFirst | (BLOCK_SIZE - 1));
                            final long count = Math.min(rangeLast - sourceFirst + 1, blockEnd - destinationFirst + 1);
                            final long delta = destinationFirst - sourceFirst + deltaAt(destinationFirst);
                            if (delta != 0) {
                                pieces.add(new long[] {sourceFirst, sourceFirst + count - 1, delta});
                            }
                            destination.add(count);
                            sourceFirst += count;
                        }
                    });
                }
            }
            pieces.sort((a, b) -> Long.compare(a[0], b[0]));
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            for (final long[] piece : pieces) {
                builder.shiftRange(piece[0], piece[1], piece[2]);
            }
            return builder.build();
        }
    }

    private void release(final int bi, final RowSetBuilderSequential builder) {
        liveCounts[bi] = RELEASED;
        clearSparse(bi);
        releasedBlocks.set(bi);
        ++releasedBlockCount;
        final long first = (long) bi << LOG_BLOCK_SIZE;
        builder.appendRange(first, first + BLOCK_SIZE - 1);
    }

    private void updateSparse(final int bi) {
        if (liveCounts[bi] > 0 && liveCounts[bi] <= sparseLiveLimit) {
            if (!sparseBlocks.get(bi)) {
                sparseBlocks.set(bi);
                ++sparseBlockCount;
            }
        } else {
            clearSparse(bi);
        }
    }

    private void clearSparse(final int bi) {
        if (sparseBlocks.get(bi)) {
            sparseBlocks.clear(bi);
            --sparseBlockCount;
        }
    }

    /**
     * @return the blocks holding the positions below {@code nextOutputPosition}, rounding up in {@code long} so that
     *         positions near {@link Integer#MAX_VALUE} do not overflow
     */
    private static int blocksInUse(final int nextOutputPosition) {
        return (int) (((long) nextOutputPosition + BLOCK_SIZE - 1) >> LOG_BLOCK_SIZE);
    }

    private void ensureCapacity(final int nextOutputPosition) {
        final int blocksNeeded = blocksInUse(nextOutputPosition);
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
                assert liveCounts[bi] != RELEASED;
                liveCounts[bi] += delta * (int) (rangeLast - rangeFirst + 1);
                assert liveCounts[bi] >= 0;
                rangeFirst = rangeLast + 1;
            }
        });
    }
}
