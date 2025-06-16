//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.util.SafeCloseable;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

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
     * Constructs a new result with an {@link RowSetFactory#empty() empty} {@link #match() match} and {@code selection}
     * for the {@link #maybeMatch() maybe match}.
     *
     * <p>
     * Equivalent to {@code ofFast(RowSetFactory.empty(), selection)}.
     *
     * @param selection the selection
     * @return the result
     */
    public static PushdownResult maybeMatch(final WritableRowSet selection) {
        return ofFast(RowSetFactory.empty(), selection);
    }

    /**
     * Constructs a new result with the exact the {@code selection} as {@link #match() match} and an
     * {@link RowSetFactory#empty() empty} {@link #maybeMatch() maybe match}.
     *
     * <p>
     * Equivalent to {@code ofFast(selection, RowSetFactory.empty())}.
     *
     * @param selection the selection
     * @return the result
     */
    public static PushdownResult match(final WritableRowSet selection) {
        return ofFast(selection, RowSetFactory.empty());
    }

    /**
     * Constructs a new result with {@code match} and {@code maybeMatch}, which are checked to not
     * {@link RowSet#overlaps(RowSet) overlap}. Callers that are careful in their construction may prefer to call
     * {@link #ofFast(WritableRowSet, WritableRowSet)}.
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
        return ofFast(match, maybeMatch);
    }

    /**
     * Constructs a new result with {@code match} and {@code maybeMatch}, which must not {@link RowSet#overlaps(RowSet)
     * overlap}. This does <b>not</b> check if the row sets overlap.
     *
     * @param match rows that match
     * @param maybeMatch rows that might match
     * @return the result
     */
    public static PushdownResult ofFast(WritableRowSet match, WritableRowSet maybeMatch) {
        return new PushdownResult(match, maybeMatch);
    }

    /**
     * Constructs a new combined {@link PushdownResult} from the sequentially ordered {@code results}. The whole of the
     * {@link PushdownResult#match() matches} must not {@link RowSet#overlaps(RowSet) overlap} with the whole of the
     * {@link PushdownResult#maybeMatch() maybe matches}. This does <b>not</b> check if the row sets overlap.
     *
     * <p>
     * This relies on {@link RowSetFactory#union(Collection)} when considering the stream of
     * {@link PushdownResult#match() matches} and {@link PushdownResult#maybeMatch() maybe matches}.
     *
     * @param results the individual results
     * @return the new results
     */
    public static PushdownResult buildSequentialFast(final Collection<PushdownResult> results) {
        return PushdownResult.ofFast(
                RowSetFactory.union(results.stream().map(PushdownResult::match).collect(Collectors.toList())),
                RowSetFactory.union(results.stream().map(PushdownResult::maybeMatch).collect(Collectors.toList())));
    }

    private PushdownResult(
            final WritableRowSet match,
            final WritableRowSet maybeMatch) {
        this.match = Objects.requireNonNull(match);
        this.maybeMatch = Objects.requireNonNull(maybeMatch);
    }

    /**
     * Rows that are known to match. Does not have any {@link RowSet#overlaps(RowSet) overlap} with {@link #maybeMatch()
     * maybeMatch rows}.
     */
    public WritableRowSet match() {
        return match;
    }

    /**
     * Rows that may match. Does not have any {@link RowSet#overlaps(RowSet) overlap} with {@link #match() match rows}.
     */
    public WritableRowSet maybeMatch() {
        return maybeMatch;
    }

    /**
     * A finished result is one in which there are no {@link #maybeMatch() maybeMatch rows}.
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
        return ofFast(match.copy(), maybeMatch.copy());
    }

    @Override
    public void close() {
        match.close();
        maybeMatch.close();
    }
}
