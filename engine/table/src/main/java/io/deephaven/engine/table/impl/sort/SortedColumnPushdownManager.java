//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sort;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownFilterMatcher;
import io.deephaven.engine.table.impl.PushdownPredicateManager;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.SortedColumnsAttribute;
import io.deephaven.engine.table.impl.select.AbstractRangeFilter;
import io.deephaven.engine.table.impl.select.ByteRangeFilter;
import io.deephaven.engine.table.impl.select.CharRangeFilter;
import io.deephaven.engine.table.impl.select.ComparableRangeFilter;
import io.deephaven.engine.table.impl.select.DoubleRangeFilter;
import io.deephaven.engine.table.impl.select.FloatRangeFilter;
import io.deephaven.engine.table.impl.select.IntRangeFilter;
import io.deephaven.engine.table.impl.select.LongRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.RangeFilter;
import io.deephaven.engine.table.impl.select.ShortRangeFilter;
import io.deephaven.engine.table.impl.select.SingleSidedComparableRangeFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterDelegating;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.regioned.kernel.*;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongConsumer;


/**
 * A {@link PushdownPredicateManager} that uses binary search against a single sorted column.
 */
public class SortedColumnPushdownManager implements PushdownPredicateManager {

    private final ColumnSource<?> columnSource;
    private final SortColumn sortColumn;
    private final AbstractRangeFilter rangeFilter;
    private final MatchFilter matchFilter;

    private SortedColumnPushdownManager(
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final SortColumn sortColumn,
            final AbstractRangeFilter rangeFilter,
            final MatchFilter matchFilter) {
        this.columnSource = columnSource;
        this.sortColumn = sortColumn;
        this.rangeFilter = rangeFilter;
        this.matchFilter = matchFilter;
    }

    @Override
    public void estimatePushdownFilterCost(
            @NotNull final WhereFilter filter,
            @NotNull final RowSet selection,
            final boolean usePrev,
            @NotNull final PushdownFilterContext context,
            @NotNull final JobScheduler jobScheduler,
            @NotNull final LongConsumer onComplete,
            @NotNull final Consumer<Exception> onError) {
        onComplete.accept(PushdownResult.TABLE_SORTED_DATA_COST);
    }

    @Override
    public void pushdownFilter(
            @NotNull final WhereFilter filter,
            @NotNull final RowSet selection,
            final boolean usePrev,
            @NotNull final PushdownFilterContext context,
            final long costCeiling,
            @NotNull final JobScheduler jobScheduler,
            @NotNull final Consumer<PushdownResult> onComplete,
            @NotNull final Consumer<Exception> onError) {
        if (selection.isEmpty()) {
            // Nothing can match an empty selection.
            onComplete.accept(PushdownResult.noneMatch(selection));
            return;
        }

        if (matchFilter != null) {
            final Class<?> dataType = columnSource.getType();
            final Object[] values = matchFilter.getValues();
            try (final RowSet matching =
                    binarySearchMatch(columnSource, dataType, selection, sortColumn, values, usePrev)) {
                // Handle normal / inverted match filters:
                if (matchFilter.getMatchOptions().inverted()) {
                    try (final RowSet pushdownMatches = selection.minus(matching)) {
                        onComplete.accept(PushdownResult.of(selection, pushdownMatches, RowSetFactory.empty()));
                    }
                } else {
                    onComplete.accept(PushdownResult.of(selection, matching, RowSetFactory.empty()));
                }
                return;
            }
        }

        if (rangeFilter != null) {
            try (final RowSet matching = binarySearchRange(columnSource, selection, sortColumn, rangeFilter, usePrev)) {
                onComplete.accept(PushdownResult.of(selection, matching, RowSetFactory.empty()));
                return;
            }
        }

        // Not able to push down the filter.
        onComplete.accept(PushdownResult.allMaybeMatch(selection));
    }

    /**
     * Helper method to call correct kernel based on data type.
     */
    public static RowSet binarySearchMatch(
            @NotNull final ColumnSource<?> source,
            @NotNull final Class<?> dataType,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            @NotNull final Object[] searchValues,
            final boolean usePrev) {

        if (dataType == char.class || dataType == Character.class) {
            return CharColumnBinarySearchKernel.binarySearchMatch(source, selection, sortColumn, searchValues, usePrev);
        }
        if (dataType == byte.class || dataType == Byte.class) {
            return ByteColumnBinarySearchKernel.binarySearchMatch(source, selection, sortColumn, searchValues, usePrev);
        }
        if (dataType == short.class || dataType == Short.class) {
            return ShortColumnBinarySearchKernel.binarySearchMatch(source, selection, sortColumn, searchValues,
                    usePrev);
        }
        if (dataType == int.class || dataType == Integer.class) {
            return IntColumnBinarySearchKernel.binarySearchMatch(source, selection, sortColumn, searchValues, usePrev);
        }
        if (dataType == long.class || dataType == Long.class) {
            return LongColumnBinarySearchKernel.binarySearchMatch(source, selection, sortColumn, searchValues, usePrev);
        }
        if (dataType == float.class || dataType == Float.class) {
            return FloatColumnBinarySearchKernel.binarySearchMatch(source, selection, sortColumn, searchValues,
                    usePrev);
        }
        if (dataType == double.class || dataType == Double.class) {
            return DoubleColumnBinarySearchKernel.binarySearchMatch(source, selection, sortColumn, searchValues,
                    usePrev);
        }
        return ObjectColumnBinarySearchKernel.binarySearchMatch(source, selection, sortColumn, searchValues, usePrev);
    }

    /**
     * Helper method to call correct search method based on filter type.
     */
    public static RowSet binarySearchRange(
            @NotNull final ColumnSource<?> source,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            @NotNull final AbstractRangeFilter rangeFilter,
            final boolean usePrev) {
        final ColumnSource<?> maybeReinterpreted = ReinterpretUtils.maybeConvertToPrimitive(source);

        if (rangeFilter instanceof CharRangeFilter) {
            return CharColumnBinarySearchKernel.binsearchRangeFilter(
                    maybeReinterpreted, selection, sortColumn, (CharRangeFilter) rangeFilter, usePrev);
        }
        if (rangeFilter instanceof ByteRangeFilter) {
            return ByteColumnBinarySearchKernel.binsearchRangeFilter(
                    maybeReinterpreted, selection, sortColumn, (ByteRangeFilter) rangeFilter, usePrev);
        }
        if (rangeFilter instanceof ShortRangeFilter) {
            return ShortColumnBinarySearchKernel.binsearchRangeFilter(
                    maybeReinterpreted, selection, sortColumn, (ShortRangeFilter) rangeFilter, usePrev);
        }
        if (rangeFilter instanceof IntRangeFilter) {
            return IntColumnBinarySearchKernel.binsearchRangeFilter(
                    maybeReinterpreted, selection, sortColumn, (IntRangeFilter) rangeFilter, usePrev);
        }
        if (rangeFilter instanceof LongRangeFilter) {
            return LongColumnBinarySearchKernel.binsearchRangeFilter(
                    maybeReinterpreted, selection, sortColumn, (LongRangeFilter) rangeFilter, usePrev);
        }
        if (rangeFilter instanceof FloatRangeFilter) {
            return FloatColumnBinarySearchKernel.binsearchRangeFilter(
                    maybeReinterpreted, selection, sortColumn, (FloatRangeFilter) rangeFilter, usePrev);
        }
        if (rangeFilter instanceof DoubleRangeFilter) {
            return DoubleColumnBinarySearchKernel.binsearchRangeFilter(
                    maybeReinterpreted, selection, sortColumn, (DoubleRangeFilter) rangeFilter, usePrev);
        }
        // Use the original-typed source.
        return ObjectColumnBinarySearchKernel.binsearchRangeFilter(source, selection, sortColumn, rangeFilter, usePrev);
    }

    /**
     * DH-23750 (42.x only): Object column types whose {@code compareTo} is consistent with {@code equals}. The Object
     * binary search kernels navigate by {@code compareTo} but accept a match by {@code equals} at a single landing
     * position, which is only correct when {@code compareTo(x) == 0} implies {@code equals(x)}.
     */
    private static final Set<Class<?>> COMPARE_CONSISTENT_WITH_EQUALS_TYPES = Set.of(
            String.class, Boolean.class, BigInteger.class, Instant.class,
            LocalDate.class, LocalTime.class, LocalDateTime.class, ZonedDateTime.class);

    /**
     * DH-23750 (42.x only): returns {@code true} when sorted-column pushdown is blocked for this filter against a
     * sorted column of {@code dataType} by one of the {@link QueryTable#DISABLE_WHERE_PUSHDOWN_SORTED_FLOATING_POINT},
     * {@link QueryTable#DISABLE_WHERE_PUSHDOWN_SORTED_CHAR_NULL_MATCH} or
     * {@link QueryTable#DISABLE_WHERE_PUSHDOWN_SORTED_INCONSISTENT_OBJECT_MATCH} configuration items. Sorted pushdown
     * emits exact results, so nothing downstream would correct a wrong answer; blocked filters are left to regular
     * filtering. Used by both the table-level manager ({@link #wrap}) and the parquet region-level sorted actions.
     *
     * @param dataType the column's data type
     * @param matchFilter the match filter being pushed down, or {@code null}
     * @param rangeFilter the range filter being pushed down, or {@code null}
     * @return whether sorted-column pushdown must be declined
     */
    public static boolean isKnownIncorrectForSortedPushdown(
            @NotNull final Class<?> dataType,
            @Nullable final MatchFilter matchFilter,
            @Nullable final AbstractRangeFilter rangeFilter) {
        if (dataType == float.class || dataType == Float.class
                || dataType == double.class || dataType == Double.class) {
            return QueryTable.DISABLE_WHERE_PUSHDOWN_SORTED_FLOATING_POINT;
        }
        if (matchFilter == null) {
            return false;
        }
        final Object[] values = matchFilter.getValues();
        if (dataType == char.class || dataType == Character.class) {
            return QueryTable.DISABLE_WHERE_PUSHDOWN_SORTED_CHAR_NULL_MATCH
                    && values != null && values.length > 1
                    && Arrays.stream(values).anyMatch(value -> value == null
                            || (value instanceof Character && (Character) value == QueryConstants.NULL_CHAR));
        }
        return QueryTable.DISABLE_WHERE_PUSHDOWN_SORTED_INCONSISTENT_OBJECT_MATCH
                && !dataType.isPrimitive() && !TypeUtils.isBoxedType(dataType)
                && !COMPARE_CONSISTENT_WITH_EQUALS_TYPES.contains(dataType);
    }

    private static boolean isSupportedRangeFilter(final AbstractRangeFilter rangeFilter) {
        return rangeFilter instanceof ByteRangeFilter
                || rangeFilter instanceof ShortRangeFilter
                || rangeFilter instanceof IntRangeFilter
                || rangeFilter instanceof LongRangeFilter
                || rangeFilter instanceof FloatRangeFilter
                || rangeFilter instanceof DoubleRangeFilter
                || rangeFilter instanceof CharRangeFilter
                || rangeFilter instanceof ComparableRangeFilter
                || rangeFilter instanceof SingleSidedComparableRangeFilter;
    }

    public static PushdownFilterMatcher wrap(
            @NotNull final QueryTable sourceTable,
            @NotNull final WhereFilter filter,
            @NotNull final List<ColumnSource<?>> filterSources,
            PushdownFilterMatcher executor) {
        if (QueryTable.DISABLE_WHERE_PUSHDOWN_SORTED_COLUMN_LOCATION
                || filterSources.size() != 1
                || filter.getColumns().size() != 1) {
            return executor;
        }

        final WhereFilter effectiveFilter = WhereFilterDelegating.maybeUnwrapFilter(filter);
        final AbstractRangeFilter rangeFilter = RangeFilter.extractRangeFilter(effectiveFilter).orElse(null);
        final MatchFilter matchFilter = MatchFilter.extractMatchFilter(effectiveFilter).orElse(null);
        if (rangeFilter == null && matchFilter == null) {
            return executor;
        }

        final MatchOptions matchOptions = matchFilter == null ? null : matchFilter.getMatchOptions();
        final boolean supportedMatchFilter = matchOptions != null
                && !matchOptions.caseInsensitive();
        final boolean supportedRangeFilter = rangeFilter != null
                && isSupportedRangeFilter(rangeFilter);
        if (!supportedRangeFilter && !supportedMatchFilter) {
            return executor;
        }
        if (isKnownIncorrectForSortedPushdown(filterSources.get(0).getType(), matchFilter, rangeFilter)) {
            // DH-23750 (42.x only): see isKnownIncorrectForSortedPushdown.
            return executor;
        }

        final String filterColumn = filter.getColumns().get(0);
        final SortColumn sortColumn = SortedColumnsAttribute.getOrderForColumn(sourceTable, filterColumn)
                .map(order -> order.isAscending()
                        ? SortColumn.asc(ColumnName.of(filterColumn))
                        : SortColumn.desc(ColumnName.of(filterColumn)))
                .orElse(null);
        if (sortColumn == null) {
            return executor;
        }
        return new SortedColumnPushdownManager(filterSources.get(0), sortColumn, rangeFilter, matchFilter);
    }
}


