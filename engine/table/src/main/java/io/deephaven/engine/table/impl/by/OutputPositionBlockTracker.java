//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.rowset.RowSequence;
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
 * to the block again. New states are only ever assigned after every existing one. Runs of sparse blocks, separated by
 * nothing but released blocks, can be collapsed, moving their states down while keeping them in order, so that the
 * blocks this empties are released too.
 * </p>
 */
final class OutputPositionBlockTracker {
    private static final int BLOCK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;
    private static final int LOG_BLOCK_SIZE = Integer.numberOfTrailingZeros(BLOCK_SIZE);
    private static final long INDEX_MASK = BLOCK_SIZE - 1;
    private static final short RELEASED = -1;

    static {
        // live counts run from 0 through BLOCK_SIZE, and are stored as shorts
        assert BLOCK_SIZE <= Short.MAX_VALUE;
    }

    /** The number of live states in each block, or {@link #RELEASED}. */
    private short[] liveCounts = new short[0];
    /** Every position in the blocks below this one has been assigned. */
    private int closedBlocks;
    /** The blocks whose live count is {@link #RELEASED}; a released block is never assigned or moved onto again. */
    private final BitSet releasedBlocks = new BitSet();
    /** A closed block with at most this many live states is sparse, and may be collapsed. */
    private final short sparseLiveLimit;
    /** The closed blocks that hold live states, but no more than {@link #sparseLiveLimit}. */
    private final BitSet sparseBlocks = new BitSet();
    /** The number of blocks in {@link #sparseBlocks}. */
    private int sparseBlockCount;

    /**
     * @param nextOutputPosition the next output position that will be assigned. Every position before it holds a live
     *        state: the initial build assigns positions contiguously from 0, and reclaiming never applies with initial
     *        groups, the only way a position could be assigned to a state not in the result.
     * @param collapseFreeFraction a closed block at least this fraction free is sparse, and runs of sparse blocks
     *        separated only by released blocks are collapsed; 1 or more disables collapsing
     */
    OutputPositionBlockTracker(final int nextOutputPosition, final double collapseFreeFraction) {
        // a full block has nothing to give up, so a sparse block has at least one free position
        sparseLiveLimit = collapseFreeFraction >= 1 ? 0
                : (short) Math.min(BLOCK_SIZE - 1, (int) (BLOCK_SIZE * (1 - collapseFreeFraction)));
        ensureCapacity(nextOutputPosition);
        closedBlocks = nextOutputPosition >> LOG_BLOCK_SIZE;
        Arrays.fill(liveCounts, 0, closedBlocks, (short) BLOCK_SIZE);
        if (closedBlocks < liveCounts.length) {
            liveCounts[closedBlocks] = (short) (nextOutputPosition & (BLOCK_SIZE - 1));
        }
        // every closed block is full, so none is sparse
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
        addLive(added);

        // A block's live count can only reach zero through a removal, or be zero already when it closes. The removed
        // positions are visited in increasing order, so each block they touch is finished once the walk moves past
        // it; blocks that closed this cycle are finished afterward, which keeps the released blocks in order.
        final int oldClosedBlocks = closedBlocks;
        final int newClosedBlocks = nextOutputPosition >> LOG_BLOCK_SIZE;
        closedBlocks = newClosedBlocks;
        final RowSetBuilderSequential releasedBuilder = RowSetFactory.builderSequential();
        final MutableInt currentBlock = new MutableInt(-1);
        removed.forAllRowKeyRanges((first, last) -> {
            long rangeFirst = first;
            while (rangeFirst <= last) {
                final int bi = (int) (rangeFirst >> LOG_BLOCK_SIZE);
                final long rangeLast = Math.min(last, ((long) bi << LOG_BLOCK_SIZE) + BLOCK_SIZE - 1);
                if (bi != currentBlock.get()) {
                    finishRemovedBlock(currentBlock.get(), oldClosedBlocks, releasedBuilder);
                    currentBlock.set(bi);
                }
                assert liveCounts[bi] != RELEASED;
                liveCounts[bi] -= (short) (rangeLast - rangeFirst + 1);
                assert liveCounts[bi] >= 0;
                rangeFirst = rangeLast + 1;
            }
        });
        finishRemovedBlock(currentBlock.get(), oldClosedBlocks, releasedBuilder);
        for (int bi = oldClosedBlocks; bi < newClosedBlocks; ++bi) {
            finishClosedBlock(bi, releasedBuilder);
        }
        return releasedBuilder.build();
    }

    /**
     * Finish a block whose states were removed this cycle, unless it closed this cycle, in which case it is finished
     * with the other newly closed blocks.
     */
    private void finishRemovedBlock(final int bi, final int oldClosedBlocks,
            final RowSetBuilderSequential releasedBuilder) {
        if (bi >= 0 && bi < oldClosedBlocks) {
            finishClosedBlock(bi, releasedBuilder);
        }
    }

    private void finishClosedBlock(final int bi, final RowSetBuilderSequential releasedBuilder) {
        if (liveCounts[bi] == 0) {
            release(bi, releasedBuilder);
        } else {
            updateSparse(bi);
        }
    }

    /**
     * Collapse runs of sparse blocks: a run is two or more sparse blocks with nothing but released blocks between them.
     * Within each run, the live states are packed into the run's sparse blocks, preserving their order, so that the
     * sparse blocks at the end of the run are left empty and can be released. Nothing moves onto a released block,
     * whose storage may already be gone or be released after this cycle. States outside the runs do not move.
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
        final List<Run> collapsedRuns = new ArrayList<>();
        final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
        final RowSetBuilderSequential releasedBuilder = RowSetFactory.builderSequential();
        long remainingShifts = maxShiftedStates;

        // find the runs first, since collapsing a run changes the sparse set
        final int[] sparse = sparseBlocks.stream().toArray();
        final List<int[]> runs = new ArrayList<>();
        int runStart = 0;
        for (int si = 1; si <= sparse.length; ++si) {
            // the next sparse block continues the run if every block before it, back to the last one, is released
            if (si < sparse.length && releasedBlocks.nextClearBit(sparse[si - 1] + 1) == sparse[si]) {
                continue;
            }
            if (si - runStart >= 2) {
                runs.add(Arrays.copyOfRange(sparse, runStart, si));
            }
            runStart = si;
        }

        for (final int[] run : runs) {
            // take as long a prefix of the run as the shift budget allows
            long runLive = 0;
            int prefix = 0;
            while (prefix < run.length && runLive + liveCounts[run[prefix]] <= remainingShifts) {
                runLive += liveCounts[run[prefix]];
                ++prefix;
            }
            final int blocksAfter = (int) ((runLive + BLOCK_SIZE - 1) >> LOG_BLOCK_SIZE);
            if (prefix < 2 || blocksAfter >= prefix) {
                continue;
            }
            remainingShifts -= runLive;

            final Run collapsed = new Run(Arrays.copyOf(run, prefix), runLive);
            collapsedRuns.add(collapsed);
            try (final RowSequence runStates =
                    liveStates.getRowSequenceByKeyRange(collapsed.firstPosition(), collapsed.lastPosition())) {
                final MutableLong rank = new MutableLong(0);
                runStates.forAllRowKeyRanges((first, last) -> {
                    long source = first;
                    while (source <= last) {
                        // the destinations are consecutive within a block, so a range splits only where one ends
                        final long destination = collapsed.position(rank.get());
                        final long count = Math.min(last - source + 1, BLOCK_SIZE - (rank.get() & INDEX_MASK));
                        shiftBuilder.shiftRange(source, source + count - 1, destination - source);
                        rank.add(count);
                        source += count;
                    }
                });
            }

            for (int bi = 0; bi < prefix; ++bi) {
                final int block = collapsed.blocks[bi];
                clearSparse(block);
                final long blockLive = Math.max(0, Math.min(BLOCK_SIZE, runLive - ((long) bi << LOG_BLOCK_SIZE)));
                liveCounts[block] = (short) blockLive;
                if (blockLive == 0) {
                    release(block, releasedBuilder);
                } else {
                    updateSparse(block);
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
     * A collapsed run: the sparse blocks its live states are packed into, in order, and the number of those states. The
     * state of rank {@code r} among the run's live states moves to position {@code r} of the run's blocks taken
     * together.
     */
    private static final class Run {
        private final int[] blocks;
        private final long live;

        private Run(final int[] blocks, final long live) {
            this.blocks = blocks;
            this.live = live;
        }

        /** @return the first position of the run's first block */
        long firstPosition() {
            return (long) blocks[0] << LOG_BLOCK_SIZE;
        }

        /** @return the last position of the run's last block, past any released blocks before it */
        long lastPosition() {
            return ((long) (blocks[blocks.length - 1] + 1) << LOG_BLOCK_SIZE) - 1;
        }

        /** @return the position, after the collapse, of the live state of rank {@code rank} within the run */
        long position(final long rank) {
            return ((long) blocks[(int) (rank >> LOG_BLOCK_SIZE)] << LOG_BLOCK_SIZE) + (rank & INDEX_MASK);
        }
    }

    /**
     * The runs collapsed in one cycle. Within a run, the live states keep their order and are packed into the run's
     * first blocks.
     */
    static final class Collapse {
        static final Collapse NONE = new Collapse(RowSetShiftData.EMPTY, List.of());

        /** The shifts that move the live states; nonempty exactly when some run was collapsed. */
        final RowSetShiftData shift;
        private final List<Run> runs;

        private Collapse(final RowSetShiftData shift, final List<Run> runs) {
            this.shift = shift;
            this.runs = runs;
        }

        /**
         * @return the number of positions in {@code rowSet} before {@code key}
         */
        private static long rankOf(final RowSet rowSet, final long key) {
            final long found = rowSet.find(key);
            return found >= 0 ? found : -found - 1;
        }

        /**
         * Apply the collapse to the live states and to row sets of states within them. Each run of the live states is
         * replaced by one range per block it fills, which is far cheaper than applying {@link #shift} range by range
         * when the live states are scattered.
         *
         * @param liveStates the output positions of the live states, before the collapse
         * @param subsets row sets of positions that are all in {@code liveStates}, such as the states added or modified
         *        this cycle
         */
        void apply(final WritableRowSet liveStates, final WritableRowSet... subsets) {
            for (final Run run : runs) {
                final long first = run.firstPosition();
                final long last = run.lastPosition();
                final long firstRank = rankOf(liveStates, first);
                for (final WritableRowSet subset : subsets) {
                    final RowSetBuilderSequential moved = RowSetFactory.builderSequential();
                    try (final RowSequence moving = subset.getRowSequenceByKeyRange(first, last)) {
                        if (moving.isEmpty()) {
                            continue;
                        }
                        moving.forAllRowKeys(key -> moved.appendKey(run.position(liveStates.find(key) - firstRank)));
                    }
                    subset.removeRange(first, last);
                    try (final RowSet movedKeys = moved.build()) {
                        subset.insert(movedKeys);
                    }
                }
                liveStates.removeRange(first, last);
                for (long rank = 0; rank < run.live; rank += BLOCK_SIZE) {
                    final long blockFirst = run.position(rank);
                    liveStates.insertRange(blockFirst, blockFirst + Math.min(BLOCK_SIZE, run.live - rank) - 1);
                }
            }
        }
    }

    private void release(final int bi, final RowSetBuilderSequential releasedBuilder) {
        liveCounts[bi] = RELEASED;
        clearSparse(bi);
        releasedBlocks.set(bi);
        final long first = (long) bi << LOG_BLOCK_SIZE;
        releasedBuilder.appendRange(first, first + BLOCK_SIZE - 1);
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

    /**
     * Count the states at {@code positions} as live in their blocks.
     */
    private void addLive(final RowSet positions) {
        positions.forAllRowKeyRanges((first, last) -> {
            long rangeFirst = first;
            while (rangeFirst <= last) {
                final int bi = (int) (rangeFirst >> LOG_BLOCK_SIZE);
                final long blockLast = ((long) bi << LOG_BLOCK_SIZE) + BLOCK_SIZE - 1;
                final long rangeLast = Math.min(last, blockLast);
                assert liveCounts[bi] != RELEASED;
                liveCounts[bi] += (short) (rangeLast - rangeFirst + 1);
                assert liveCounts[bi] <= BLOCK_SIZE;
                rangeFirst = rangeLast + 1;
            }
        });
    }
}
