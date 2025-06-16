//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSet;
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
     * The selection.
     */
    private final WritableRowSet selection;

    /**
     * Rows that match the predicate.
     */
    private final WritableRowSet match;

    /**
     * Rows that might match the predicate but would need to be tested to be certain.
     */
    private final WritableRowSet maybeMatch;

    /**
     * Constructs a new result with {@code selection} as all of {@link #maybeMatch()}.
     *
     * <p>
     * Equivalent to {@code ofUnsafe(selection, RowSetFactory.empty(), selection.copy())}.
     *
     * @param selection the selection
     * @return the result
     */
    public static PushdownResult maybeMatch(final WritableRowSet selection) {
        return ofUnsafe(selection, RowSetFactory.empty(), selection.copy());
    }

    /**
     * Constructs a new result with {@code selection} as all of {@link #match()}.
     *
     * <p>
     * Equivalent to {@code ofUnsafe(selection, selection.copy(), RowSetFactory.empty())}.
     *
     * @param selection the selection
     * @return the result
     */
    public static PushdownResult match(final WritableRowSet selection) {
        return ofUnsafe(selection, selection.copy(), RowSetFactory.empty());
    }

    /**
     * Constructs a new result with {@code selection} as all of {@link #noMatch()}.
     *
     * <p>
     * Equivalent to {@code ofUnsafe(selection, RowSetFactory.empty(), RowSetFactory.empty())}.
     *
     * @param selection the selection
     * @return the result
     */
    public static PushdownResult noMatch(final WritableRowSet selection) {
        return ofUnsafe(selection, RowSetFactory.empty(), RowSetFactory.empty());
    }

    /**
     * Constructs a new result with {@code selection}, {@code match}, and {@code maybeMatch}. {@code match} and
     * {@code maybeMatch} must be a (possibly improper) subset of {@code selection}. which are checked to not
     * {@link RowSet#overlaps(RowSet) overlap}. Callers that are careful in their construction may prefer to call
     * {@link #ofUnsafe(WritableRowSet, WritableRowSet, WritableRowSet)}.
     *
     * @param match rows that match
     * @param maybeMatch rows that might match
     * @return the result
     */
    public static PushdownResult of(
            final WritableRowSet selection,
            final WritableRowSet match,
            final WritableRowSet maybeMatch) {
        if (!match.subsetOf(selection)) {
            throw new IllegalArgumentException("match must be a subset of selection");
        }
        if (!maybeMatch.subsetOf(selection)) {
            throw new IllegalArgumentException("maybeMatch must be a subset of selection");
        }
        if (match.overlaps(maybeMatch)) {
            throw new IllegalArgumentException("match and maybeMatch should be non-overlapping row sets");
        }
        return new PushdownResult(selection, match, maybeMatch);
    }

    /**
     * Constructs a new result with {@code selection}, {@code match}, and {@code maybeMatch}. which must not
     * {@link RowSet#overlaps(RowSet) overlap}. This does <b>not</b> check if the row sets overlap.
     *
     * @param selection the selection
     * @param match rows that match
     * @param maybeMatch rows that might match
     * @return the result
     */
    public static PushdownResult ofUnsafe(
            final WritableRowSet selection,
            final WritableRowSet match,
            final WritableRowSet maybeMatch) {
        final long matchSize = match.size();
        final long maybeMatchSize = maybeMatch.size();
        final long selectionSize = selection.size();
        if (matchSize + maybeMatchSize > selectionSize) {
            throw new IllegalArgumentException(
                    String.format("Invalid PushdownResult, matchSize + maybeMatchSize > selectionSize, %d + %d > %d",
                            matchSize, maybeMatchSize, selectionSize));
        }
        return new PushdownResult(selection, match, maybeMatch);
    }

    private PushdownResult(
            final WritableRowSet selection,
            final WritableRowSet match,
            final WritableRowSet maybeMatch) {
        this.selection = Objects.requireNonNull(selection);
        this.match = Objects.requireNonNull(match);
        this.maybeMatch = Objects.requireNonNull(maybeMatch);
    }

    /**
     * The set of rows considered for this pushdown result. Each row key from this selection will be in exactly one of
     * {@link #match() match}, {@link #maybeMatch() maybeMatch}, or {@link #noMatch() noMatch}.
     */
    public WritableRowSet selection() {
        return selection;
    }

    /**
     * Rows that are known to match. Is a {@link RowSet#subsetOf(RowSet) subset of} {@link #selection() selection}. Does
     * not {@link RowSet#overlaps(RowSet) overlap} with {@link #maybeMatch() maybeMatch} nor {@link #noMatch() noMatch}.
     */
    public WritableRowSet match() {
        return match;
    }

    /**
     * Rows that may match. Is a {@link RowSet#subsetOf(RowSet) subset of} {@link #selection() selection}. Does not
     * {@link RowSet#overlaps(RowSet) overlap} with {@link #match() match} nor {@link #noMatch() noMatch}.
     */
    public WritableRowSet maybeMatch() {
        return maybeMatch;
    }

    /**
     * Rows that are known to <b>not</b> match. Is a {@link RowSet#subsetOf(RowSet) subset of} {@link #selection()
     * selection}. Does not {@link RowSet#overlaps(RowSet) overlap} with {@link #match() match} nor {@link #maybeMatch()
     * maybeMatch}.
     *
     * <p>
     * Note: this result is computed as {@code selection - match - maybeMatch}.
     */
    @SuppressWarnings("unused")
    public WritableRowSet noMatch() {
        final WritableRowSet noMatch = selection.copy();
        if (match.isNonempty()) {
            noMatch.remove(match);
        }
        if (maybeMatch.isNonempty()) {
            noMatch.remove(maybeMatch);
        }
        return noMatch;
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
     * @return the copy
     */
    public PushdownResult copy() {
        return new PushdownResult(selection.copy(), match.copy(), maybeMatch.copy());
    }

    @Override
    public void close() {
        SafeCloseable.closeAll(selection, match, maybeMatch);
    }
}
