//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sort;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ElementSource;
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
import io.deephaven.engine.table.impl.sources.regioned.kernel.*;
import io.deephaven.engine.table.impl.util.JobScheduler;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static io.deephaven.util.QueryConstants.MAX_BYTE;
import static io.deephaven.util.QueryConstants.MAX_CHAR;
import static io.deephaven.util.QueryConstants.MAX_DOUBLE;
import static io.deephaven.util.QueryConstants.MAX_FLOAT;
import static io.deephaven.util.QueryConstants.MAX_INT;
import static io.deephaven.util.QueryConstants.MAX_LONG;
import static io.deephaven.util.QueryConstants.MAX_SHORT;
import static io.deephaven.util.QueryConstants.NULL_BYTE;
import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.QueryConstants.NULL_SHORT;

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
            @NotNull final ElementSource<?> source,
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
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            @NotNull final AbstractRangeFilter rangeFilter,
            final boolean usePrev) {
        if (rangeFilter instanceof CharRangeFilter) {
            return binarySearchRangeChar(source, selection, sortColumn, rangeFilter, usePrev);
        }
        if (rangeFilter instanceof ByteRangeFilter) {
            return binarySearchRangeByte(source, selection, sortColumn, rangeFilter, usePrev);
        }
        if (rangeFilter instanceof ShortRangeFilter) {
            return binarySearchRangeShort(source, selection, sortColumn, rangeFilter, usePrev);
        }
        if (rangeFilter instanceof IntRangeFilter) {
            return binarySearchRangeInt(source, selection, sortColumn, rangeFilter, usePrev);
        }
        if (rangeFilter instanceof LongRangeFilter) {
            return binarySearchRangeLong(source, selection, sortColumn, rangeFilter, usePrev);
        }
        if (rangeFilter instanceof FloatRangeFilter) {
            return binarySearchRangeFloat(source, selection, sortColumn, rangeFilter, usePrev);
        }
        if (rangeFilter instanceof DoubleRangeFilter) {
            return binarySearchRangeDouble(source, selection, sortColumn, rangeFilter, usePrev);
        }
        return binarySearchRangeObject(source, selection, sortColumn, rangeFilter, usePrev);
    }

    private static RowSet binarySearchRangeChar(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            @NotNull final AbstractRangeFilter filter,
            final boolean usePrev) {
        final CharRangeFilter rangeFilter = (CharRangeFilter) filter;
        if (rangeFilter.getLower() == NULL_CHAR && rangeFilter.isLowerInclusive()) {
            return CharColumnBinarySearchKernel.binarySearchMax(
                    source, selection, sortColumn,
                    rangeFilter.getUpper(), rangeFilter.isUpperInclusive(), usePrev);
        } else if (rangeFilter.getUpper() == MAX_CHAR && rangeFilter.isUpperInclusive()) {
            return CharColumnBinarySearchKernel.binarySearchMin(
                    source, selection, sortColumn,
                    rangeFilter.getLower(), rangeFilter.isLowerInclusive(), usePrev);
        } else {
            return CharColumnBinarySearchKernel.binarySearchMinMax(
                    source, selection, sortColumn,
                    rangeFilter.getLower(), rangeFilter.getUpper(),
                    rangeFilter.isLowerInclusive(), rangeFilter.isUpperInclusive(), usePrev);
        }
    }

    private static RowSet binarySearchRangeByte(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            @NotNull final AbstractRangeFilter filter,
            final boolean usePrev) {
        final ByteRangeFilter rangeFilter = (ByteRangeFilter) filter;
        if (rangeFilter.getLower() == NULL_BYTE && rangeFilter.isLowerInclusive()) {
            return ByteColumnBinarySearchKernel.binarySearchMax(
                    source, selection, sortColumn,
                    rangeFilter.getUpper(), rangeFilter.isUpperInclusive(), usePrev);
        } else if (rangeFilter.getUpper() == MAX_BYTE && rangeFilter.isUpperInclusive()) {
            return ByteColumnBinarySearchKernel.binarySearchMin(
                    source, selection, sortColumn,
                    rangeFilter.getLower(), rangeFilter.isLowerInclusive(), usePrev);
        } else {
            return ByteColumnBinarySearchKernel.binarySearchMinMax(
                    source, selection, sortColumn,
                    rangeFilter.getLower(), rangeFilter.getUpper(),
                    rangeFilter.isLowerInclusive(), rangeFilter.isUpperInclusive(), usePrev);
        }
    }

    private static RowSet binarySearchRangeShort(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            @NotNull final AbstractRangeFilter filter,
            final boolean usePrev) {
        final ShortRangeFilter rangeFilter = (ShortRangeFilter) filter;
        if (rangeFilter.getLower() == NULL_SHORT && rangeFilter.isLowerInclusive()) {
            return ShortColumnBinarySearchKernel.binarySearchMax(
                    source, selection, sortColumn,
                    rangeFilter.getUpper(), rangeFilter.isUpperInclusive(), usePrev);
        } else if (rangeFilter.getUpper() == MAX_SHORT && rangeFilter.isUpperInclusive()) {
            return ShortColumnBinarySearchKernel.binarySearchMin(
                    source, selection, sortColumn,
                    rangeFilter.getLower(), rangeFilter.isLowerInclusive(), usePrev);
        } else {
            return ShortColumnBinarySearchKernel.binarySearchMinMax(
                    source, selection, sortColumn,
                    rangeFilter.getLower(), rangeFilter.getUpper(),
                    rangeFilter.isLowerInclusive(), rangeFilter.isUpperInclusive(), usePrev);
        }
    }

    private static RowSet binarySearchRangeInt(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            @NotNull final AbstractRangeFilter filter,
            final boolean usePrev) {
        final IntRangeFilter rangeFilter = (IntRangeFilter) filter;
        if (rangeFilter.getLower() == NULL_INT && rangeFilter.isLowerInclusive()) {
            return IntColumnBinarySearchKernel.binarySearchMax(
                    source, selection, sortColumn,
                    rangeFilter.getUpper(), rangeFilter.isUpperInclusive(), usePrev);
        } else if (rangeFilter.getUpper() == MAX_INT && rangeFilter.isUpperInclusive()) {
            return IntColumnBinarySearchKernel.binarySearchMin(
                    source, selection, sortColumn,
                    rangeFilter.getLower(), rangeFilter.isLowerInclusive(), usePrev);
        } else {
            return IntColumnBinarySearchKernel.binarySearchMinMax(
                    source, selection, sortColumn,
                    rangeFilter.getLower(), rangeFilter.getUpper(),
                    rangeFilter.isLowerInclusive(), rangeFilter.isUpperInclusive(), usePrev);
        }
    }

    private static RowSet binarySearchRangeLong(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            @NotNull final AbstractRangeFilter filter,
            final boolean usePrev) {
        final LongRangeFilter rangeFilter = (LongRangeFilter) filter;
        if (rangeFilter.getLower() == NULL_LONG && rangeFilter.isLowerInclusive()) {
            return LongColumnBinarySearchKernel.binarySearchMax(
                    source, selection, sortColumn,
                    rangeFilter.getUpper(), rangeFilter.isUpperInclusive(), usePrev);
        } else if (rangeFilter.getUpper() == MAX_LONG && rangeFilter.isUpperInclusive()) {
            return LongColumnBinarySearchKernel.binarySearchMin(
                    source, selection, sortColumn,
                    rangeFilter.getLower(), rangeFilter.isLowerInclusive(), usePrev);
        } else {
            return LongColumnBinarySearchKernel.binarySearchMinMax(
                    source, selection, sortColumn,
                    rangeFilter.getLower(), rangeFilter.getUpper(),
                    rangeFilter.isLowerInclusive(), rangeFilter.isUpperInclusive(), usePrev);
        }
    }

    private static RowSet binarySearchRangeFloat(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            @NotNull final AbstractRangeFilter filter,
            final boolean usePrev) {
        final FloatRangeFilter rangeFilter = (FloatRangeFilter) filter;
        if (rangeFilter.getLower() == NULL_FLOAT && rangeFilter.isLowerInclusive()) {
            return FloatColumnBinarySearchKernel.binarySearchMax(
                    source, selection, sortColumn,
                    rangeFilter.getUpper(), rangeFilter.isUpperInclusive(), usePrev);
        } else if (rangeFilter.getUpper() == MAX_FLOAT && rangeFilter.isUpperInclusive()) {
            return FloatColumnBinarySearchKernel.binarySearchMin(
                    source, selection, sortColumn,
                    rangeFilter.getLower(), rangeFilter.isLowerInclusive(), usePrev);
        } else {
            return FloatColumnBinarySearchKernel.binarySearchMinMax(
                    source, selection, sortColumn,
                    rangeFilter.getLower(), rangeFilter.getUpper(),
                    rangeFilter.isLowerInclusive(), rangeFilter.isUpperInclusive(), usePrev);
        }
    }

    private static RowSet binarySearchRangeDouble(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            @NotNull final AbstractRangeFilter filter,
            final boolean usePrev) {
        final DoubleRangeFilter rangeFilter = (DoubleRangeFilter) filter;
        if (rangeFilter.getLower() == NULL_DOUBLE && rangeFilter.isLowerInclusive()) {
            return DoubleColumnBinarySearchKernel.binarySearchMax(
                    source, selection, sortColumn,
                    rangeFilter.getUpper(), rangeFilter.isUpperInclusive(), usePrev);
        } else if (rangeFilter.getUpper() == MAX_DOUBLE && rangeFilter.isUpperInclusive()) {
            return DoubleColumnBinarySearchKernel.binarySearchMin(
                    source, selection, sortColumn,
                    rangeFilter.getLower(), rangeFilter.isLowerInclusive(), usePrev);
        } else {
            return DoubleColumnBinarySearchKernel.binarySearchMinMax(
                    source, selection, sortColumn,
                    rangeFilter.getLower(), rangeFilter.getUpper(),
                    rangeFilter.isLowerInclusive(), rangeFilter.isUpperInclusive(), usePrev);
        }
    }

    private static RowSet binarySearchRangeObject(
            @NotNull final ElementSource<?> source,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            @NotNull final AbstractRangeFilter filter,
            final boolean usePrev) {
        if (filter instanceof SingleSidedComparableRangeFilter) {
            final SingleSidedComparableRangeFilter rangeFilter = (SingleSidedComparableRangeFilter) filter;
            if (rangeFilter.isGreaterThan()) {
                return ObjectColumnBinarySearchKernel.binarySearchMin(
                        source, selection, sortColumn,
                        rangeFilter.getPivot(), rangeFilter.isLowerInclusive(), usePrev);
            } else {
                return ObjectColumnBinarySearchKernel.binarySearchMax(
                        source, selection, sortColumn,
                        rangeFilter.getPivot(), rangeFilter.isUpperInclusive(), usePrev);
            }
        }
        final ComparableRangeFilter rangeFilter = (ComparableRangeFilter) filter;
        return ObjectColumnBinarySearchKernel.binarySearchMinMax(
                source, selection, sortColumn,
                rangeFilter.getLower(), rangeFilter.getUpper(),
                rangeFilter.isLowerInclusive(), rangeFilter.isUpperInclusive(), usePrev);
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


