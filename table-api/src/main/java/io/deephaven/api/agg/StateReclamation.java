//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * How a keyed aggregation reclaims the state of a group whose rows have all been removed.
 *
 * <p>
 * A group that empties leaves the result at the end of the update cycle. Without reclaiming, its state is kept, so a
 * group that returns gets its row key back, and memory grows with every group ever seen. With reclaiming, its state is
 * removed, so a group that returns is a new group, and gets a row key after every existing row's; the storage for row
 * keys whose groups have all been removed is freed.
 *
 * <p>
 * Reclaiming applies only to a refreshing aggregation whose operators can all reclaim states, and which neither
 * preserves empty groups nor has initial groups. An aggregation given {@link #releaseBlocks(double)} explicitly fails
 * if it cannot reclaim; one given {@link #configured()} does not reclaim instead. An aggregation of static, add-only,
 * append-only, or blink input never empties a group, so it accepts any choice and has nothing to reclaim.
 */
@Immutable
@SimpleStyle
public abstract class StateReclamation {

    /**
     * The kinds of reclamation.
     */
    public enum Kind {
        /**
         * The engine's configured reclamation, which does not reclaim when the aggregation cannot.
         */
        CONFIGURED,
        /**
         * Never reclaim states.
         */
        NONE,
        /**
         * Reclaim states, freeing the storage for runs of row keys whose groups have all been removed.
         */
        RELEASE_BLOCKS
    }

    /**
     * @return the engine's configured reclamation; an aggregation that cannot reclaim states does not, rather than
     *         failing
     */
    public static StateReclamation configured() {
        return ImmutableStateReclamation.of(Kind.CONFIGURED, 1.0);
    }

    /**
     * @return the reclamation that keeps every state, so that a group's row key never changes
     */
    public static StateReclamation none() {
        return ImmutableStateReclamation.of(Kind.NONE, 1.0);
    }

    /**
     * Equivalent to {@code releaseBlocks(1.0)}, which never moves a row.
     *
     * @return the reclamation that frees the storage for runs of row keys whose groups have all been removed
     */
    public static StateReclamation releaseBlocks() {
        return releaseBlocks(1.0);
    }

    /**
     * @param collapseFreeFraction a block of row keys is sparse when at least this fraction of its row keys belongs to
     *        removed groups. The rows of a run of sparse blocks, separated only by blocks whose groups have all been
     *        removed, may move to lower row keys, keeping their order, so that the storage for the blocks this empties
     *        can be freed. It is clamped to the range 0 to 1, and 1 never moves rows.
     * @return the reclamation that frees the storage for runs of row keys whose groups have all been removed
     */
    public static StateReclamation releaseBlocks(final double collapseFreeFraction) {
        // NaN is left for the check to reject, since it has no place in the range to clamp to
        final double clamped =
                Double.isNaN(collapseFreeFraction) ? collapseFreeFraction
                        : Math.max(0.0, Math.min(1.0, collapseFreeFraction));
        return ImmutableStateReclamation.of(Kind.RELEASE_BLOCKS, clamped);
    }

    /**
     * @return the kind of reclamation
     */
    @Parameter
    public abstract Kind kind();

    /**
     * @return for {@link Kind#RELEASE_BLOCKS}, the fraction of a block of row keys that must belong to removed groups
     *         before its rows may move; 1 for the other kinds
     */
    @Parameter
    public abstract double collapseFreeFraction();

    @Check
    final void checkCollapseFreeFraction() {
        if (Double.isNaN(collapseFreeFraction())) {
            throw new IllegalArgumentException("collapseFreeFraction must not be NaN");
        }
        if (kind() != Kind.RELEASE_BLOCKS && collapseFreeFraction() != 1.0) {
            throw new IllegalArgumentException(kind() + " has no collapseFreeFraction");
        }
    }
}
