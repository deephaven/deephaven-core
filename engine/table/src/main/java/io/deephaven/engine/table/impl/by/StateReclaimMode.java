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
 * <li>{@link #releaseBlocks(double, double, boolean)}: the storage for a block of output positions is released once all
 * of its states are removed. States move only to collapse runs of sparse blocks or to shift blocks down over released
 * ones, as the parameters allow.</li>
 * </ul>
 * Only the modes that move states ({@link #movesStates()}) change a group's row key while it has rows; a consumer that
 * looks up a group's current row key and reads previous values there needs a mode that does not.
 * <p>
 * A mode that reclaims states applies only to a refreshing aggregation whose operators can all reclaim states, and that
 * neither preserves empty groups nor has initial groups. An aggregation given such a mode explicitly fails if it cannot
 * reclaim; one given the {@link #configured()} mode uses {@link #none()} instead.
 */
public final class StateReclaimMode {

    private static final StateReclaimMode NONE = new StateReclaimMode(false, 1, -1, false, false);

    private final boolean reclaim;
    private final double collapseFreeFraction;
    private final double blockShiftFraction;
    private final boolean bulkShift;
    private final boolean configured;

    private StateReclaimMode(final boolean reclaim, final double collapseFreeFraction,
            final double blockShiftFraction, final boolean bulkShift, final boolean configured) {
        this.reclaim = reclaim;
        this.collapseFreeFraction = collapseFreeFraction;
        this.blockShiftFraction = blockShiftFraction;
        this.bulkShift = bulkShift;
        this.configured = configured;
    }

    /**
     * @return the mode that never removes states
     */
    public static StateReclaimMode none() {
        return NONE;
    }

    /**
     * @param collapseFreeFraction a closed block of output positions at least this fraction free is sparse, and runs of
     *        adjacent sparse blocks are collapsed so that the blocks this empties can be released; 1 or more never
     *        collapses
     * @param blockShiftFraction once the released blocks are at least this fraction of the output positions assigned,
     *        the blocks after released ones are shifted down over them, keeping the states in order, so that output
     *        positions are reused; zero shifts for any released block, and negative never shifts
     * @param bulkShift whether the block shift waits until it can shift every block after the first released one in one
     *        cycle, giving the released blocks back at the end, rather than sweeping toward the end over several
     *        cycles. Moves are then paid for by a credit that each cycle's added and removed states earn, carried
     *        across cycles; otherwise each cycle may move no more states than its input rows.
     * @return the mode that releases the storage for blocks of output positions whose states have all been removed
     * @throws IllegalArgumentException if either fraction is NaN
     */
    public static StateReclaimMode releaseBlocks(final double collapseFreeFraction, final double blockShiftFraction,
            final boolean bulkShift) {
        return releaseBlocks(collapseFreeFraction, blockShiftFraction, bulkShift, false);
    }

    private static StateReclaimMode releaseBlocks(final double collapseFreeFraction, final double blockShiftFraction,
            final boolean bulkShift, final boolean configured) {
        // every comparison with NaN is false, so a NaN fraction would mean different things in different places
        if (Double.isNaN(collapseFreeFraction) || Double.isNaN(blockShiftFraction)) {
            throw new IllegalArgumentException("State reclaim fractions must not be NaN: collapseFreeFraction="
                    + collapseFreeFraction + ", blockShiftFraction=" + blockShiftFraction);
        }
        return new StateReclaimMode(true, collapseFreeFraction, blockShiftFraction, bulkShift, configured);
    }

    /**
     * The mode configured by the {@code ChunkedOperatorAggregationHelper} properties, as they are now. An aggregation
     * that cannot reclaim states uses {@link #none()} instead of this mode, rather than failing.
     *
     * @return the configured mode
     */
    public static StateReclaimMode configured() {
        if (!ChunkedOperatorAggregationHelper.RECLAIM_STATES) {
            return NONE;
        }
        return releaseBlocks(ChunkedOperatorAggregationHelper.COLLAPSE_FREE_FRACTION,
                ChunkedOperatorAggregationHelper.BLOCK_SHIFT_FRACTION, ChunkedOperatorAggregationHelper.BULK_SHIFT,
                true);
    }

    /**
     * @return whether this mode came from {@link #configured()}, so that an aggregation that cannot reclaim states uses
     *         {@link #none()} instead of failing
     */
    public boolean isConfigured() {
        return configured;
    }

    /**
     * @return whether states are removed from the result and the hash table when their groups empty
     */
    public boolean reclaims() {
        return reclaim;
    }

    /**
     * @return the fraction free at which a block of output positions is collapsed
     */
    public double collapseFreeFraction() {
        return collapseFreeFraction;
    }

    /**
     * @return the fraction of output positions released at which blocks are shifted down
     */
    public double blockShiftFraction() {
        return blockShiftFraction;
    }

    /**
     * @return whether blocks shift only in bulk, with moves paid for by credit carried across cycles
     */
    public boolean bulkShift() {
        return bulkShift;
    }

    /**
     * @return whether a state's output position may change while its group has rows
     */
    public boolean movesStates() {
        return reclaim && (collapseFreeFraction < 1 || blockShiftFraction >= 0);
    }

    @Override
    public String toString() {
        if (!reclaim) {
            return "StateReclaimMode{none}";
        }
        return "StateReclaimMode{releaseBlocks, collapseFreeFraction=" + collapseFreeFraction
                + ", blockShiftFraction=" + blockShiftFraction + ", bulkShift=" + bulkShift
                + (configured ? ", configured" : "") + '}';
    }
}
