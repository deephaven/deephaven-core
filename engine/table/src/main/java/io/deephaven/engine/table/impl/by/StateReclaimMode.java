//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

/**
 * How an incremental aggregation reclaims the states of groups whose rows have all been removed.
 * <p>
 * A group that empties leaves the result at the end of the cycle. Unless the mode is {@link #none()}, its state leaves
 * the hash table as well, so a group that returns on a later cycle is a new state and, like any new group, gets a row
 * key after every existing row's. The modes differ in whether the storage of removed states is freed:
 * <ul>
 * <li>{@link #none()}: states are never removed. An empty group keeps its state, and a returning group gets its row key
 * back. Memory grows with every group ever seen.</li>
 * <li>{@link #releaseBlocks(double)}: when enough adjacent states are removed, free the underlying storage. If the
 * {@code collapseFreeFraction} is less than 1.0, states may move to eliminate fragmentation that would otherwise
 * prevent empty runs from being freed.</li>
 * </ul>
 * Only a mode that moves states ({@link #movesStates()}) changes a group's row key while it has rows; a consumer that
 * looks up a group's current row key and reads previous values there needs a mode that does not.
 * <p>
 * A mode that reclaims states applies only to a refreshing aggregation whose operators can all reclaim states, and that
 * neither preserves empty groups nor has initial groups. An aggregation given such a mode explicitly fails if it cannot
 * reclaim; one given the {@link #configured()} mode uses {@link #none()} instead.
 */
public final class StateReclaimMode {

    private static final StateReclaimMode NONE = new StateReclaimMode(false, 1, false);

    private final boolean reclaim;
    private final double collapseFreeFraction;
    private final boolean configured;

    private StateReclaimMode(final boolean reclaim, final double collapseFreeFraction, final boolean configured) {
        this.reclaim = reclaim;
        this.collapseFreeFraction = collapseFreeFraction;
        this.configured = configured;
    }

    /**
     * @return the mode that never removes states
     */
    public static StateReclaimMode none() {
        return NONE;
    }

    /**
     * @param collapseFreeFraction when at least this fraction of a run of row keys belongs to removed groups, the rows
     *        that remain may move to lower row keys, keeping their order, so that the storage for the row keys this
     *        empties can be freed; 1 or more never moves rows
     * @return the mode that frees the storage for runs of adjacent row keys whose groups have all been removed
     * @throws IllegalArgumentException if {@code collapseFreeFraction} is NaN
     */
    public static StateReclaimMode releaseBlocks(final double collapseFreeFraction) {
        return releaseBlocks(collapseFreeFraction, false);
    }

    private static StateReclaimMode releaseBlocks(final double collapseFreeFraction, final boolean configured) {
        // every comparison with NaN is false, so a NaN fraction would neither collapse nor be rejected as out of range
        if (Double.isNaN(collapseFreeFraction)) {
            throw new IllegalArgumentException("collapseFreeFraction must not be NaN");
        }
        return new StateReclaimMode(true, collapseFreeFraction, configured);
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
        return releaseBlocks(ChunkedOperatorAggregationHelper.COLLAPSE_FREE_FRACTION, true);
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
     * @return the fraction of a run of row keys that must belong to removed groups before the remaining rows may move
     */
    public double collapseFreeFraction() {
        return collapseFreeFraction;
    }

    /**
     * @return whether a group's row key may change while it has rows
     */
    public boolean movesStates() {
        return reclaim && collapseFreeFraction < 1;
    }

    @Override
    public String toString() {
        if (!reclaim) {
            return "StateReclaimMode{none}";
        }
        return "StateReclaimMode{releaseBlocks, collapseFreeFraction=" + collapseFreeFraction
                + (configured ? ", configured" : "") + '}';
    }
}
