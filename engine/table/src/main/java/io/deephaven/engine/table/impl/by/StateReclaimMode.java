//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

/**
 * How an incremental aggregation reclaims the states of groups whose rows have all been removed.
 * <p>
 * A group that empties leaves the result and the hash table at the end of the cycle, unless the mode is
 * {@link #none()}. A group that returns on a later cycle is a new state, after every existing one. The modes differ in
 * how the output positions of removed states are given back:
 * <ul>
 * <li>{@link #none()}: states are never removed. An empty state keeps its output position, and a returning group reuses
 * it. Memory grows with every group ever seen.</li>
 * <li>{@link #releaseBlocks(double, double)}: the storage for a block of output positions is released once all of its
 * states are removed. States move only if the collapse or block shift is enabled.</li>
 * <li>{@link #compact()}: the states after removed ones are shifted down into their positions.</li>
 * </ul>
 * Only the modes that move states ({@link #movesStates()}) change a group's row key while it has rows; a consumer that
 * looks up a group's current row key and reads previous values there needs a mode that does not.
 * <p>
 * The mode applies only when every operator of the aggregation can reclaim states, and never with preserved empty
 * groups or initial groups.
 */
public final class StateReclaimMode {

    private static final StateReclaimMode NONE = new StateReclaimMode(false, false, 1, -1);
    private static final StateReclaimMode COMPACT = new StateReclaimMode(true, false, 1, -1);

    private final boolean reclaim;
    private final boolean releaseBlocks;
    private final double collapseFreeFraction;
    private final double blockShiftFraction;

    private StateReclaimMode(final boolean reclaim, final boolean releaseBlocks, final double collapseFreeFraction,
            final double blockShiftFraction) {
        this.reclaim = reclaim;
        this.releaseBlocks = releaseBlocks;
        this.collapseFreeFraction = collapseFreeFraction;
        this.blockShiftFraction = blockShiftFraction;
    }

    /**
     * @return the mode that never removes states
     */
    public static StateReclaimMode none() {
        return NONE;
    }

    /**
     * @return the mode that shifts the states after removed ones down into their output positions
     */
    public static StateReclaimMode compact() {
        return COMPACT;
    }

    /**
     * @param collapseFreeFraction a block of output positions at least this fraction free is sparse, and runs of
     *        adjacent sparse blocks are collapsed so that their emptied blocks can be released; 1 or more never
     *        collapses
     * @param blockShiftFraction once the released blocks are at least this fraction of the output positions assigned,
     *        the blocks after released ones are shifted down over them, keeping the states in order, so that output
     *        positions are reused; zero shifts for any released block, and negative never shifts
     * @return the mode that releases the storage for blocks of output positions whose states have all been removed
     * @throws IllegalArgumentException if either fraction is NaN
     */
    public static StateReclaimMode releaseBlocks(final double collapseFreeFraction, final double blockShiftFraction) {
        // every comparison with NaN is false, so a NaN fraction would mean different things in different places
        if (Double.isNaN(collapseFreeFraction) || Double.isNaN(blockShiftFraction)) {
            throw new IllegalArgumentException("State reclaim fractions must not be NaN: collapseFreeFraction="
                    + collapseFreeFraction + ", blockShiftFraction=" + blockShiftFraction);
        }
        return new StateReclaimMode(true, true, collapseFreeFraction, blockShiftFraction);
    }

    /**
     * @return the mode configured by the {@code ChunkedOperatorAggregationHelper} properties, as they are now
     */
    public static StateReclaimMode configured() {
        if (!ChunkedOperatorAggregationHelper.RECLAIM_STATES) {
            return NONE;
        }
        if (!ChunkedOperatorAggregationHelper.RELEASE_BLOCKS) {
            return COMPACT;
        }
        return releaseBlocks(ChunkedOperatorAggregationHelper.COLLAPSE_FREE_FRACTION,
                ChunkedOperatorAggregationHelper.BLOCK_SHIFT_FRACTION);
    }

    /**
     * @return whether states are removed from the result and the hash table when their groups empty
     */
    public boolean reclaims() {
        return reclaim;
    }

    /**
     * @return whether the storage for whole blocks of removed states is released, rather than the result compacted
     */
    public boolean releasesBlocks() {
        return releaseBlocks;
    }

    /**
     * @return the fraction free at which a block of output positions is collapsed, when releasing blocks
     */
    public double collapseFreeFraction() {
        return collapseFreeFraction;
    }

    /**
     * @return the fraction of output positions released at which blocks are shifted down, when releasing blocks
     */
    public double blockShiftFraction() {
        return blockShiftFraction;
    }

    /**
     * @return whether a state's output position may change while its group has rows
     */
    public boolean movesStates() {
        return reclaim && (!releaseBlocks || collapseFreeFraction < 1 || blockShiftFraction >= 0);
    }

    @Override
    public String toString() {
        if (!reclaim) {
            return "StateReclaimMode{none}";
        }
        if (!releaseBlocks) {
            return "StateReclaimMode{compact}";
        }
        return "StateReclaimMode{releaseBlocks, collapseFreeFraction=" + collapseFreeFraction
                + ", blockShiftFraction=" + blockShiftFraction + '}';
    }
}
