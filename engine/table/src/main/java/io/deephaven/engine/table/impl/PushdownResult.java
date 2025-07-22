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

    // Heuristic cost estimates for different push-down operations to find matching rows.
    // Larger numbers indicate operations that are expected to touch more data or incur higher I/O latency; the values
    // are strictly relative.
    /**
     * Only table/row-group statistics are checked, assuming the metadata is already loaded
     */
    public static final long METADATA_STATS_COST = 10_000L;
    /**
     * Column-level Bloom filter needs to be used
     */
    public static final long BLOOM_FILTER_COST = 20_000L;
    /**
     * Requires querying an in-memory index structure
     */
    public static final long IN_MEMORY_DATA_INDEX_COST = 30_000L;
    /**
     * Requires using binary search on sorted data
     */
    public static final long SORTED_DATA_COST = 40_000L;
    /**
     * Requires reading and querying an external index table
     */
    public static final long DEFERRED_DATA_INDEX_COST = 50_000L;

    /**
     * The selection. Retaining selection here makes the pushdown result "self-contained" which helps with composability
     * and potential safety checks when results are combined.
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
     * Constructs a new result with all of {@code selection} as {@link #maybeMatch() maybeMatch}.
     *
     * @param selection the selection
     * @return the result
     */
    public static PushdownResult maybeMatch(final RowSet selection) {
        return new PushdownResult(selection.copy(), RowSetFactory.empty(), selection.copy());
    }

    /**
     * Constructs a new result with all of {@code selection} as {@link #match() match}.
     *
     * @param selection the selection
     * @return the result
     */
    public static PushdownResult match(final RowSet selection) {
        return new PushdownResult(selection.copy(), selection.copy(), RowSetFactory.empty());
    }

    /**
     * Constructs a new result with all of {@code selection} as {@link #noMatchCopy() noMatch}.
     *
     * @param selection the selection
     * @return the result
     */
    public static PushdownResult noMatch(final RowSet selection) {
        return new PushdownResult(selection.copy(), RowSetFactory.empty(), RowSetFactory.empty());
    }

    /**
     * Constructs a new result with {@code selection}, {@code match}, and {@code maybeMatch}. {@code match} and
     * {@code maybeMatch} must be non-overlapping subsets of {@code selection}, which is checked via
     * {@link RowSet#overlaps(RowSet)} and {@link RowSet#subsetOf(RowSet)}. Callers that are careful in their
     * construction may prefer to call {@link #ofUnsafe(RowSet, RowSet, RowSet)}.
     *
     * @param selection the selection
     * @param match rows that match
     * @param maybeMatch rows that might match
     * @return the result
     */
    public static PushdownResult of(
            final RowSet selection,
            final RowSet match,
            final RowSet maybeMatch) {
        if (!match.subsetOf(selection)) {
            throw new IllegalArgumentException("match must be a subset of selection");
        }
        if (!maybeMatch.subsetOf(selection)) {
            throw new IllegalArgumentException("maybeMatch must be a subset of selection");
        }
        if (match.overlaps(maybeMatch)) {
            throw new IllegalArgumentException("match and maybeMatch should be non-overlapping row sets");
        }
        return new PushdownResult(selection.copy(), match.copy(), maybeMatch.copy());
    }

    /**
     * Constructs a new result with {@code selection}, {@code match}, and {@code maybeMatch}. {@code match} and
     * {@code maybeMatch} must be non-overlapping subsets of {@code selection}, but this is <b>not</b> thoroughly check.
     *
     * @param selection the selection
     * @param match rows that match
     * @param maybeMatch rows that might match
     * @return the result
     */
    public static PushdownResult ofUnsafe(
            final RowSet selection,
            final RowSet match,
            final RowSet maybeMatch) {
        // return of(selection, match, maybeMatch);
        final long matchSize = match.size();
        final long maybeMatchSize = maybeMatch.size();
        final long selectionSize = selection.size();
        if (matchSize + maybeMatchSize > selectionSize) {
            throw new IllegalArgumentException(
                    String.format("Invalid PushdownResult, matchSize + maybeMatchSize > selectionSize, %d + %d > %d",
                            matchSize, maybeMatchSize, selectionSize));
        }
        return new PushdownResult(selection.copy(), match.copy(), maybeMatch.copy());
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
     * {@link #match() match}, {@link #maybeMatch() maybeMatch}, or {@link #noMatchCopy() noMatch}. Ownership does
     * <b>not</b> transfer to the caller.
     */
    public WritableRowSet selection() {
        return selection;
    }

    /**
     * Rows that are known to match. Is a {@link RowSet#subsetOf(RowSet) subset of} {@link #selection() selection}. Does
     * not {@link RowSet#overlaps(RowSet) overlap} with {@link #maybeMatch() maybeMatch} nor {@link #noMatchCopy()
     * noMatch}. Ownership does <b>not</b> transfer to the caller.
     */
    public WritableRowSet match() {
        return match;
    }

    /**
     * Rows that may match. Is a {@link RowSet#subsetOf(RowSet) subset of} {@link #selection() selection}. Does not
     * {@link RowSet#overlaps(RowSet) overlap} with {@link #match() match} nor {@link #noMatchCopy() noMatch}. Ownership
     * does <b>not</b> transfer to the caller.
     */
    public WritableRowSet maybeMatch() {
        return maybeMatch;
    }

    /**
     * Rows that are known to <b>not</b> match. Is a {@link RowSet#subsetOf(RowSet) subset of} {@link #selection()
     * selection}. Does not {@link RowSet#overlaps(RowSet) overlap} with {@link #match() match} nor {@link #maybeMatch()
     * maybeMatch}. Ownership <b>does</b> transfer to the caller.
     *
     * <p>
     * Note: this result is computed as {@code selection - match - maybeMatch} and is provided for documentation and
     * completeness purposes, and is not typically used outside of testing or debugging.
     */
    @SuppressWarnings("unused")
    public WritableRowSet noMatchCopy() {
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
     * The size of {@link #noMatchCopy() noMatch}.
     */
    public long noMatchSize() {
        return selection.size() - match.size() - maybeMatch.size();
    }

    /**
     * A finished result is one in which there are no {@link #maybeMatch() maybeMatch rows}.
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

    /**
     * Closes {@link #selection()}, {@link #match()}, and {@link #maybeMatch()}.
     */
    @Override
    public void close() {
        SafeCloseable.closeAll(selection, match, maybeMatch);
    }
}
