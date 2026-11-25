//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;

/**
 * Match and range filters that fail a test when the engine scans with them. A pushdown that answers every row exactly
 * leaves the engine no rows to scan, so a {@code where} with one of these filters succeeds only if the column source's
 * pushdown answers the filter. A {@code where} that falls back to scanning, or a pushdown that leaves rows that only
 * maybe match, calls {@code filter} or {@code filterInverse}, which throw.
 */
public final class NonScanningFilters {

    private NonScanningFilters() {}

    /** A match filter whose scan fails the test. The caller initializes the filter. */
    public static MatchFilter match(
            @NotNull final MatchOptions options,
            @NotNull final String columnName,
            final Object... values) {
        return new NonScanningMatchFilter(options, columnName, values);
    }

    /**
     * An {@link InstantRangeFilter} on epoch nanoseconds whose scan fails the test. The caller initializes the filter.
     */
    public static WhereFilter instantRange(
            @NotNull final String columnName,
            final long lower,
            final long upper,
            final boolean lowerInclusive,
            final boolean upperInclusive) {
        return new NonScanningInstantRangeFilter(columnName, lower, upper, lowerInclusive, upperInclusive);
    }

    /** A {@link LongRangeFilter} whose scan fails the test. The caller initializes the filter. */
    public static WhereFilter longRange(
            @NotNull final String columnName,
            final long lower,
            final long upper,
            final boolean lowerInclusive,
            final boolean upperInclusive) {
        return new NonScanningLongRangeFilter(columnName, lower, upper, lowerInclusive, upperInclusive);
    }

    private static AssertionError scanned(final WhereFilter filter) {
        return new AssertionError("Scanned with " + filter + " instead of answering from a pushdown");
    }

    private static AssertionError copied(final WhereFilter filter) {
        return new AssertionError("Copied " + filter + ", and a copy would scan without failing the test");
    }

    private static final class NonScanningMatchFilter extends MatchFilter {
        private NonScanningMatchFilter(
                final MatchOptions options,
                final String columnName,
                final Object... values) {
            super(options, columnName, values);
        }

        @NotNull
        @Override
        public WritableRowSet filter(
                @NotNull final RowSet selection,
                @NotNull final RowSet fullSet,
                @NotNull final Table table,
                final boolean usePrev) {
            throw scanned(this);
        }

        @NotNull
        @Override
        public WritableRowSet filterInverse(
                @NotNull final RowSet selection,
                @NotNull final RowSet fullSet,
                @NotNull final Table table,
                final boolean usePrev) {
            throw scanned(this);
        }

        @Override
        public WhereFilter copy() {
            throw copied(this);
        }
    }

    private static final class NonScanningInstantRangeFilter extends InstantRangeFilter {
        private NonScanningInstantRangeFilter(
                final String columnName,
                final long lower,
                final long upper,
                final boolean lowerInclusive,
                final boolean upperInclusive) {
            super(columnName, lower, upper, lowerInclusive, upperInclusive);
        }

        @NotNull
        @Override
        public WritableRowSet filter(
                @NotNull final RowSet selection,
                @NotNull final RowSet fullSet,
                @NotNull final Table table,
                final boolean usePrev) {
            throw scanned(this);
        }

        @NotNull
        @Override
        public WritableRowSet filterInverse(
                @NotNull final RowSet selection,
                @NotNull final RowSet fullSet,
                @NotNull final Table table,
                final boolean usePrev) {
            throw scanned(this);
        }

        @Override
        public InstantRangeFilter copy() {
            throw copied(this);
        }
    }

    private static final class NonScanningLongRangeFilter extends LongRangeFilter {
        private NonScanningLongRangeFilter(
                final String columnName,
                final long lower,
                final long upper,
                final boolean lowerInclusive,
                final boolean upperInclusive) {
            super(columnName, lower, upper, lowerInclusive, upperInclusive);
        }

        @NotNull
        @Override
        public WritableRowSet filter(
                @NotNull final RowSet selection,
                @NotNull final RowSet fullSet,
                @NotNull final Table table,
                final boolean usePrev) {
            throw scanned(this);
        }

        @NotNull
        @Override
        public WritableRowSet filterInverse(
                @NotNull final RowSet selection,
                @NotNull final RowSet fullSet,
                @NotNull final Table table,
                final boolean usePrev) {
            throw scanned(this);
        }

        @Override
        public LongRangeFilter copy() {
            throw copied(this);
        }
    }
}
