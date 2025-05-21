//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.util.SafeCloseable;

import java.util.Objects;

/**
 * Records the results of a push-down predicate filter operation.
 */
public final class PushdownResult implements SafeCloseable {
    /**
     * Costs for various types of push-down operations.
     */
    public static long METADATA_STATS_COST = 10_000L;
    public static long BLOOM_FILTER_COST = 20_000L;
    public static long IN_MEMORY_DATA_INDEX_COST = 30_000L;
    public static long SORTED_DATA_COST = 40_000L;
    public static long DEFERRED_DATA_INDEX_COST = 50_000L;

    /**
     * Rows that match the predicate.
     */
    private final WritableRowSet match;

    /**
     * Rows that might match the predicate but would need to be tested to be certain.
     */
    private final WritableRowSet maybeMatch;

    /**
     * Constructs a new result with an {@link RowSetFactory#empty() empty} {@link #match() match} and a
     * {@link RowSet#copy() copy} of {@code selection} for the {@link #maybeMatch() maybe match}.
     *
     * <p>
     * Equivalent to {@code of(RowSetFactory.empty(), selection.copy())}.
     *
     * @param selection the selection
     * @return the result
     */
    public static PushdownResult maybeMatch(final RowSet selection) {
        return new PushdownResult(RowSetFactory.empty(), selection.copy());
    }

    /**
     * Constructs a new result with the exact the {@code selection} as {@link #match() match} and an
     * {@link RowSetFactory#empty() empty} {@link #maybeMatch() maybe match}.
     *
     * <p>
     * Equivalent to {@code of(selection, RowSetFactory.empty())}.
     *
     * @param selection the selection
     * @return the result
     */
    public static PushdownResult match(final WritableRowSet selection) {
        return new PushdownResult(selection, RowSetFactory.empty());
    }

    /**
     * Constructs a new result with {@code match} and {@code maybeMatch}, which must not {@link RowSet#overlaps(RowSet)
     * overlap}.
     *
     * @param match rows that match
     * @param maybeMatch rows that might match
     * @return the result
     */
    public static PushdownResult of(
            final WritableRowSet match,
            final WritableRowSet maybeMatch) {
        if (match.overlaps(maybeMatch)) {
            throw new IllegalArgumentException("match and maybeMatch should be non-overlapping row sets");
        }
        return new PushdownResult(match, maybeMatch);
    }

    public static Builder builder() {
        return new Builder();
    }

    private PushdownResult(
            final WritableRowSet match,
            final WritableRowSet maybeMatch) {
        this.match = Objects.requireNonNull(match);
        this.maybeMatch = Objects.requireNonNull(maybeMatch);
    }

    public WritableRowSet match() {
        return match;
    }

    public WritableRowSet maybeMatch() {
        return maybeMatch;
    }

    /**
     * A finished result is one in which there are no {@link #maybeMatch()} rows. As such, there is no room for it to
     * {@link #promote(RowSet)} or to {@link #drop(RowSet)}.
     *
     * <p>
     * Equivalent to {@code maybeMatch().isEmpty()}.
     *
     * @return if the result is finished
     */
    public boolean isFinished() {
        return maybeMatch.isEmpty();
    }

    /**
     * Creates a copy of {@code this}.
     *
     * <p>
     * Equivalent to {@code of(match().copy(), maybeMatch().copy())}.
     *
     * @return the copy
     */
    public PushdownResult copy() {
        return new PushdownResult(match.copy(), maybeMatch.copy());
    }

    /**
     * Creates a new result with {@code rowSet} {@link RowSet#union(RowSet) union'd} with {@link #match()} and
     * {@code rowSet} {@link RowSet#minus(RowSet) minus'd} from {@link #maybeMatch()}. {@code rowSet} must be a
     * {@link RowSet#subsetOf(RowSet) subset of} {@link #maybeMatch()}.
     *
     * @param rowSet the row set to promote
     * @return the new result
     */
    public PushdownResult promote(final RowSet rowSet) {
        // When toDrop is empty, this is equivalent to copy().
        // We need precision because we should never be promoting rows from maybeMatch to match that don't exist
        // Execute this first to check for precise.
        final WritableRowSet newMaybeMatch = minusPrecise(maybeMatch, rowSet);
        // Given our preconditions (match / maybeMatch are non-overlapping), we know that the union will also be
        // "precise" (all new rows wrt match).
        return new PushdownResult(match.union(rowSet), newMaybeMatch);
    }

    /**
     * Creates a new result with {@code rowSet} {@link RowSet#minus(RowSet) minus'd} from {@link #maybeMatch()}.
     * {@code rowSet} must be a {@link RowSet#subsetOf(RowSet) subset of} {@link #maybeMatch()}.
     *
     * @param rowSet the row set to drop
     * @return the new result
     */
    public PushdownResult drop(final RowSet rowSet) {
        // When toDrop is empty, this is equivalent to copy()
        // We need precision because we should never be removing from match
        // Execute this first to check for precise.
        final WritableRowSet newMaybeMatch = minusPrecise(maybeMatch, rowSet);
        return new PushdownResult(match.copy(), newMaybeMatch);
    }

    private static WritableRowSet minusPrecise(WritableRowSet rowSet, RowSet toRemove) {
        if (toRemove.isEmpty()) {
            return rowSet.copy();
        }
        if (!toRemove.subsetOf(rowSet)) {
            throw new IllegalArgumentException("remove here should be precise, but toRemove is not a subset of rowSet");
        }
        // minus optimization we can do only b/c we know toRemove is a subset
        if (toRemove.size() == rowSet.size()) {
            return RowSetFactory.empty();
        }
        return rowSet.minus(toRemove);
    }

    @Override
    public void close() {
        match.close();
        maybeMatch.close();
    }

    public static final class Builder {
        private final RowSetBuilderRandom matchBuilder;
        private final RowSetBuilderRandom maybeMatchBuilder;

        private Builder() {
            matchBuilder = RowSetFactory.builderRandom();
            maybeMatchBuilder = RowSetFactory.builderRandom();
        }

        public synchronized void add(final PushdownResult results) {
            if (results.match.isNonempty()) {
                matchBuilder.addRowSet(results.match);
            }
            if (results.maybeMatch.isNonempty()) {
                maybeMatchBuilder.addRowSet(results.maybeMatch);
            }
        }

        public synchronized PushdownResult build() {
            return of(matchBuilder.build(), maybeMatchBuilder.build());
        }
    }
}
