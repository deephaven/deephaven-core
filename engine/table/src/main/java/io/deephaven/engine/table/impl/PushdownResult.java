//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Records the results of a push-down predicate filter operation.
 */
public final class PushdownResult implements SafeCloseable {

    // Heuristic cost estimates for different push-down operations to find matching rows.
    // Larger numbers indicate operations that are expected to touch more data or incur higher I/O latency; the values
    // are strictly relative.
    /**
     * The entire column contains a single value, so a single read is sufficient to determine matches.
     */
    public static final long SINGLE_VALUE_COLUMN_COST = 1_000L;
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
     * Forces additional safety checks in {@link #of(RowSet, RowSet, RowSet)}. Controlled via configuration property
     * "PushdownResult.forceValidation". Not a user-documented feature. {@code false} by default.
     */
    @VisibleForTesting
    static final boolean FORCE_VALIDATION =
            Configuration.getInstance().getBooleanWithDefault("PushdownResult.forceValidation", false);

    /**
     * Rows that match the predicate.
     */
    private final WritableRowSet match;

    /**
     * Rows that might match the predicate but would need to be tested to be certain.
     */
    private final WritableRowSet maybeMatch;

    /**
     * Constructs a new result with all of {@code selection} as {@link #maybeMatch() maybeMatch}. Semantically
     * equivalent to {@code of(selection, RowSetFactory.empty(), selection)}, but this method is preferred over that
     * case as no preconditions need to be checked. {@code selection} must be the {@code selection} from
     * {@link PushdownFilterMatcher#pushdownFilter(WhereFilter, RowSet, boolean, PushdownFilterContext, long, JobScheduler, Consumer, Consumer)}.
     *
     * @param selection the selection assigned to {@link #maybeMatch() maybeMatch}
     * @return the result
     * @see #of(RowSet, RowSet, RowSet)
     */
    public static PushdownResult allMaybeMatch(@NotNull final RowSet selection) {
        try (final WritableRowSet empty = RowSetFactory.empty()) {
            return copy(empty, selection);
        }
    }

    /**
     * Constructs a new result with all of {@code selection} as {@link #match() match}. Semantically equivalent to
     * {@code of(selection, selection, RowSetFactory.empty())}, but this method is preferred over that case as no
     * preconditions need to be checked. {@code selection} must be the {@code selection} from
     * {@link PushdownFilterMatcher#pushdownFilter(WhereFilter, RowSet, boolean, PushdownFilterContext, long, JobScheduler, Consumer, Consumer)}.
     *
     * @param selection the selection assigned to {@link #match() match}
     * @return the result
     * @see #of(RowSet, RowSet, RowSet)
     */
    public static PushdownResult allMatch(@NotNull final RowSet selection) {
        try (final WritableRowSet empty = RowSetFactory.empty()) {
            return copy(selection, empty);
        }
    }

    /**
     * Constructs a new result with no {@link #match() match} nor {@link #maybeMatch() maybeMatch} rows. Semantically
     * equivalent to {@code of(selection, RowSetFactory.empty(), RowSetFactory.empty())}, but this method is preferred
     * over that case as no preconditions need to be checked. {@code selection} must be the {@code selection} from
     * {@link PushdownFilterMatcher#pushdownFilter(WhereFilter, RowSet, boolean, PushdownFilterContext, long, JobScheduler, Consumer, Consumer)}.
     *
     * @param selection the selection (representing the full "no match" case)
     * @return the result
     * @see #of(RowSet, RowSet, RowSet)
     */
    public static PushdownResult allNoMatch(@SuppressWarnings("unused") @NotNull final RowSet selection) {
        try (final WritableRowSet empty = RowSetFactory.empty()) {
            return copy(empty, empty);
        }
    }

    /**
     * Constructs a new result with {@code selection}, {@code match}, and {@code maybeMatch}. {@code match} and
     * {@code maybeMatch} must be non-overlapping subsets of {@code selection}, but this may not be thoroughly checked.
     * {@code selection} must be the {@code selection} from
     * {@link PushdownFilterMatcher#pushdownFilter(WhereFilter, RowSet, boolean, PushdownFilterContext, long, JobScheduler, Consumer, Consumer)}.
     *
     * @param selection the selection
     * @param match rows that match
     * @param maybeMatch rows that might match
     * @return the result
     */
    public static PushdownResult of(
            @NotNull final RowSet selection,
            @NotNull final RowSet match,
            @NotNull final RowSet maybeMatch) {
        if (FORCE_VALIDATION) {
            if (!match.subsetOf(selection)) {
                throw new IllegalArgumentException("match must be a subset of selection");
            }
            if (!maybeMatch.subsetOf(selection)) {
                throw new IllegalArgumentException("maybeMatch must be a subset of selection");
            }
            if (match.overlaps(maybeMatch)) {
                throw new IllegalArgumentException("match and maybeMatch should be non-overlapping row sets");
            }
        } else {
            final long matchSize = match.size();
            final long maybeMatchSize = maybeMatch.size();
            final long selectionSize = selection.size();
            if (matchSize + maybeMatchSize > selectionSize) {
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid PushdownResult, matchSize + maybeMatchSize > selectionSize, %d + %d > %d",
                                matchSize, maybeMatchSize, selectionSize));
            }
        }
        return copy(match, maybeMatch);
    }

    private static PushdownResult copy(final RowSet match, final RowSet maybeMatch) {
        // This is pedantic, but necessary for technically correct & prompt cleanup in exceptional cases.
        final WritableRowSet matchCopy = match.copy();
        try {
            final WritableRowSet maybeMatchCopy = maybeMatch.copy();
            try {
                return new PushdownResult(matchCopy, maybeMatchCopy);
            } catch (final RuntimeException e) {
                try (maybeMatchCopy) {
                    throw e;
                }
            }
        } catch (final RuntimeException e) {
            try (matchCopy) {
                throw e;
            }
        }
    }

    private PushdownResult(
            final WritableRowSet match,
            final WritableRowSet maybeMatch) {
        this.match = Objects.requireNonNull(match);
        this.maybeMatch = Objects.requireNonNull(maybeMatch);
    }

    /**
     * Rows that are known to match. Is a {@link RowSet#subsetOf(RowSet) subset of} the {@code selection} from
     * {@link PushdownFilterMatcher#pushdownFilter(WhereFilter, RowSet, boolean, PushdownFilterContext, long, JobScheduler, Consumer, Consumer)}.
     * Does not {@link RowSet#overlaps(RowSet) overlap} with {@link #maybeMatch() maybeMatch} nor {@code noMatch}.
     * Ownership does <b>not</b> transfer to the caller.
     */
    public WritableRowSet match() {
        return match;
    }

    /**
     * Rows that may match. Is a {@link RowSet#subsetOf(RowSet) subset of} the {@code selection} from
     * {@link PushdownFilterMatcher#pushdownFilter(WhereFilter, RowSet, boolean, PushdownFilterContext, long, JobScheduler, Consumer, Consumer)}.
     * Does not {@link RowSet#overlaps(RowSet) overlap} with {@link #match() match} nor {@code noMatch}. Ownership does
     * <b>not</b> transfer to the caller.
     */
    public WritableRowSet maybeMatch() {
        return maybeMatch;
    }

    /**
     * Creates a copy of {@code this}.
     *
     * @return the copy
     */
    public PushdownResult copy() {
        return copy(match, maybeMatch);
    }

    /**
     * Closes {@link #match()} and {@link #maybeMatch()}.
     */
    @Override
    public void close() {
        SafeCloseable.closeAll(match, maybeMatch);
    }
}
