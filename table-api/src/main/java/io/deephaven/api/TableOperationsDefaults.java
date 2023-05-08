/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.expression.AsOfJoinMatchFactory;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.util.ConcurrentMethod;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Sub-interface to capture default methods rom {@link TABLE}.
 */
public interface TableOperationsDefaults<TOPS extends TableOperations<TOPS, TABLE>, TABLE>
        extends TableOperations<TOPS, TABLE> {

    Filter[] ZERO_LENGTH_FILTER_ARRAY = new Filter[0];
    ColumnName[] ZERO_LENGTH_COLUMNNAME_ARRAY = new ColumnName[0];

    // -----------------------------------------------------------------------------------------------------------------
    // Sort Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS sort(String... columnsToSortBy) {
        return sort(Arrays.stream(columnsToSortBy)
                .map(ColumnName::of).map(SortColumn::asc).collect(Collectors.toList()));
    }

    @Override
    @ConcurrentMethod
    default TOPS sortDescending(String... columnsToSortBy) {
        return sort(Arrays.stream(columnsToSortBy)
                .map(ColumnName::of).map(SortColumn::desc).collect(Collectors.toList()));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Filter Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS where(String... filters) {
        return where(Filter.from(filters));
    }

    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default TOPS whereIn(TABLE rightTable, String... columnsToMatch) {
        return whereIn(rightTable, JoinMatch.from(columnsToMatch));
    }

    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default TOPS whereNotIn(TABLE rightTable, String... columnsToMatch) {
        return whereNotIn(rightTable, JoinMatch.from(columnsToMatch));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Column Selection Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS view(String... columns) {
        return view(Selectable.from(columns));
    }

    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS updateView(String... newColumns) {
        return updateView(Selectable.from((newColumns)));
    }

    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default TOPS update(String... newColumns) {
        return update(Selectable.from((newColumns)));
    }

    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default TOPS lazyUpdate(String... newColumns) {
        return lazyUpdate(Selectable.from((newColumns)));
    }

    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default TOPS select(String... columns) {
        return select(Selectable.from(columns));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Join Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default TOPS naturalJoin(TABLE rightTable, String columnsToMatch) {
        return naturalJoin(rightTable, JoinMatch.from(splitToCollection(columnsToMatch)), Collections.emptyList());
    }

    @Override
    default TOPS naturalJoin(TABLE rightTable, String columnsToMatch, String columnsToAdd) {
        return naturalJoin(rightTable, JoinMatch.from(splitToCollection(columnsToMatch)),
                JoinAddition.from(splitToCollection(columnsToAdd)));
    }

    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default TOPS exactJoin(TABLE rightTable, String columnsToMatch) {
        return exactJoin(rightTable, JoinMatch.from(splitToCollection(columnsToMatch)), Collections.emptyList());
    }


    @Override
    default TOPS exactJoin(TABLE rightTable, String columnsToMatch, String columnsToAdd) {
        return exactJoin(rightTable, JoinMatch.from(splitToCollection(columnsToMatch)),
                JoinAddition.from(TableOperationsDefaults.splitToCollection(columnsToAdd)));
    }

    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default TOPS join(TABLE rightTable, String columnsToMatch) {
        return join(rightTable, JoinMatch.from(splitToCollection(columnsToMatch)), Collections.emptyList());
    }

    @Override
    default TOPS join(TABLE rightTable, String columnsToMatch, String columnsToAdd) {
        return join(rightTable, JoinMatch.from(splitToCollection(columnsToMatch)),
                JoinAddition.from(TableOperationsDefaults.splitToCollection(columnsToAdd)));
    }

    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default TOPS aj(TABLE rightTable, String columnsToMatch) {
        final AsOfJoinMatchFactory.AsOfJoinResult result =
                AsOfJoinMatchFactory.getAjExpressions(splitToCollection(columnsToMatch));
        return aj(rightTable, Arrays.asList(result.matches), Collections.emptyList(), result.rule);
    }

    @Override
    default TOPS aj(TABLE rightTable, String columnsToMatch, String columnsToAdd) {
        final AsOfJoinMatchFactory.AsOfJoinResult result =
                AsOfJoinMatchFactory.getAjExpressions(splitToCollection(columnsToMatch));
        return aj(rightTable, Arrays.asList(result.matches), JoinAddition.from(splitToCollection(columnsToAdd)),
                result.rule);
    }

    @Override
    default TOPS aj(TABLE rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return aj(rightTable, columnsToMatch, columnsToAdd, AsOfJoinRule.LESS_THAN_EQUAL);
    }

    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default TOPS raj(TABLE rightTable, String columnsToMatch) {
        final AsOfJoinMatchFactory.ReverseAsOfJoinResult result =
                AsOfJoinMatchFactory.getRajExpressions(splitToCollection(columnsToMatch));
        return raj(rightTable, Arrays.asList(result.matches), Collections.emptyList(), result.rule);
    }

    @Override
    default TOPS raj(TABLE rightTable, String columnsToMatch, String columnsToAdd) {
        final AsOfJoinMatchFactory.ReverseAsOfJoinResult result =
                AsOfJoinMatchFactory.getRajExpressions(splitToCollection(columnsToMatch));
        return raj(rightTable, Arrays.asList(result.matches), JoinAddition.from(splitToCollection(columnsToAdd)),
                result.rule);
    }

    @Override
    default TOPS raj(TABLE rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return raj(rightTable, columnsToMatch, columnsToAdd, ReverseAsOfJoinRule.GREATER_THAN_EQUAL);
    }

    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default TOPS rangeJoin(
            final TABLE rightTable,
            final Collection<String> columnsToMatch,
            final Collection<? extends Aggregation> aggregations) {
        if (columnsToMatch.isEmpty()) {
            throw new IllegalArgumentException(
                    "No match expressions found; must include at least a range match expression");
        }
        final Iterator<String> matchExpressions = columnsToMatch.iterator();
        final int numExactMatches = columnsToMatch.size() - 1;
        final Collection<JoinMatch> exactMatches = numExactMatches == 0
                ? Collections.emptyList()
                : new ArrayList<>(numExactMatches);
        for (int emi = 0; emi < numExactMatches; ++emi) {
            exactMatches.add(JoinMatch.parse(matchExpressions.next()));
        }
        final RangeJoinMatch rangeMatch = RangeJoinMatch.parse(matchExpressions.next());
        return rangeJoin(rightTable, exactMatches, rangeMatch, aggregations);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Aggregation Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS groupBy() {
        return groupBy(Collections.emptyList());
    }

    @Override
    @ConcurrentMethod
    default TOPS groupBy(String... groupByColumns) {
        return groupBy(ColumnName.from(groupByColumns));
    }

    @Override
    @ConcurrentMethod
    default TOPS groupBy(Collection<? extends ColumnName> groupByColumns) {
        return aggAllBy(AggSpec.group(), groupByColumns.toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    // -------------------------------------------------------------------------------------------

    @Override
    default TOPS ungroup() {
        return ungroup(false, Collections.emptyList());
    }

    @Override
    default TOPS ungroup(boolean nullFill) {
        return ungroup(nullFill, Collections.emptyList());
    }

    @Override
    default TOPS ungroup(String... columnsToUngroup) {
        return ungroup(false, Arrays.stream(columnsToUngroup).map(ColumnName::of).collect(Collectors.toList()));
    }

    @Override
    default TOPS ungroup(boolean nullFill, String... columnsToUngroup) {
        return ungroup(nullFill, Arrays.stream(columnsToUngroup).map(ColumnName::of).collect(Collectors.toList()));
    }

    // -------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS aggAllBy(AggSpec spec) {
        return aggAllBy(spec, Collections.emptyList());
    }

    @Override
    @ConcurrentMethod
    default TOPS aggAllBy(AggSpec spec, String... groupByColumns) {
        return aggAllBy(spec, ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default TOPS aggAllBy(AggSpec spec, Collection<String> groupByColumns) {
        return aggAllBy(spec, ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    // -------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS aggBy(Aggregation aggregation) {
        return aggBy(Collections.singletonList(aggregation));
    }

    @Override
    @ConcurrentMethod
    default TOPS aggBy(Collection<? extends Aggregation> aggregations) {
        return aggBy(aggregations, Collections.emptyList());
    }

    @Override
    @ConcurrentMethod
    default TOPS aggBy(Collection<? extends Aggregation> aggregations, boolean preserveEmpty) {
        return aggBy(aggregations, preserveEmpty, null, Collections.emptyList());
    }

    @Override
    @ConcurrentMethod
    default TOPS aggBy(Aggregation aggregation, String... groupByColumns) {
        return aggBy(Collections.singletonList(aggregation), groupByColumns);
    }

    @Override
    @ConcurrentMethod
    default TOPS aggBy(Aggregation aggregation, Collection<? extends ColumnName> groupByColumns) {
        return aggBy(Collections.singletonList(aggregation), groupByColumns);
    }

    @Override
    @ConcurrentMethod
    default TOPS aggBy(Collection<? extends Aggregation> aggregations, String... groupByColumns) {
        return aggBy(aggregations, ColumnName.from(groupByColumns));
    }

    @Override
    @ConcurrentMethod
    default TOPS aggBy(Collection<? extends Aggregation> aggregations,
            Collection<? extends ColumnName> groupByColumns) {
        return aggBy(aggregations, AGG_BY_PRESERVE_EMPTY_DEFAULT, null, groupByColumns);
    }

    // -------------------------------------------------------------------------------------------

    @Override
    default TOPS updateBy(final UpdateByOperation operation) {
        return updateBy(UpdateByControl.defaultInstance(), Collections.singletonList(operation),
                Collections.emptyList());
    }

    @Override
    default TOPS updateBy(final Collection<? extends UpdateByOperation> operations) {
        return updateBy(UpdateByControl.defaultInstance(), operations, Collections.emptyList());
    }

    @Override
    default TOPS updateBy(final UpdateByControl control, final Collection<? extends UpdateByOperation> operations) {
        return updateBy(control, operations, Collections.emptyList());
    }

    @Override
    default TOPS updateBy(final UpdateByOperation operation, final String... byColumns) {
        return updateBy(UpdateByControl.defaultInstance(), Collections.singletonList(operation),
                ColumnName.from(byColumns));
    }

    @Override
    default TOPS updateBy(final Collection<? extends UpdateByOperation> operations, final String... byColumns) {
        return updateBy(UpdateByControl.defaultInstance(), operations, ColumnName.from(byColumns));
    }

    @Override
    default TOPS updateBy(Collection<? extends UpdateByOperation> operations,
            Collection<? extends ColumnName> byColumns) {
        return updateBy(UpdateByControl.defaultInstance(), operations, byColumns);
    }

    // -------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS selectDistinct(String... columns) {
        return selectDistinct(Selectable.from(columns));
    }

    @Override
    @ConcurrentMethod
    default TOPS selectDistinct(Selectable... columns) {
        return selectDistinct(Arrays.asList(columns));
    }

    // -------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS countBy(String countColumnName) {
        return countBy(countColumnName, ZERO_LENGTH_COLUMNNAME_ARRAY);
    }

    @Override
    @ConcurrentMethod
    default TOPS countBy(String countColumnName, String... groupByColumns) {
        return countBy(countColumnName, ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default TOPS countBy(String countColumnName, ColumnName... groupByColumns) {
        return aggBy(Aggregation.AggCount(countColumnName), Arrays.asList(groupByColumns));
    }

    @Override
    @ConcurrentMethod
    default TOPS countBy(String countColumnName, Collection<String> groupByColumns) {
        return countBy(countColumnName, ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    // -------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS firstBy() {
        return firstBy(ZERO_LENGTH_COLUMNNAME_ARRAY);
    }

    @Override
    @ConcurrentMethod
    default TOPS firstBy(String... groupByColumns) {
        return firstBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default TOPS firstBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.first(), groupByColumns);
    }

    @Override
    @ConcurrentMethod
    default TOPS firstBy(Collection<String> groupByColumns) {
        return firstBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    // -------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS lastBy() {
        return lastBy(ZERO_LENGTH_COLUMNNAME_ARRAY);
    }

    @Override
    @ConcurrentMethod
    default TOPS lastBy(String... groupByColumns) {
        return lastBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default TOPS lastBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.last(), groupByColumns);
    }

    @Override
    @ConcurrentMethod
    default TOPS lastBy(Collection<String> groupByColumns) {
        return lastBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    // -------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS minBy() {
        return minBy(ZERO_LENGTH_COLUMNNAME_ARRAY);
    }

    @Override
    @ConcurrentMethod
    default TOPS minBy(String... groupByColumns) {
        return minBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default TOPS minBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.min(), groupByColumns);
    }

    @Override
    @ConcurrentMethod
    default TOPS minBy(Collection<String> groupByColumns) {
        return minBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    // -------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS maxBy() {
        return maxBy(ZERO_LENGTH_COLUMNNAME_ARRAY);
    }

    @Override
    @ConcurrentMethod
    default TOPS maxBy(String... groupByColumns) {
        return maxBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default TOPS maxBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.max(), groupByColumns);
    }

    @Override
    @ConcurrentMethod
    default TOPS maxBy(Collection<String> groupByColumns) {
        return maxBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    // -------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS sumBy() {
        return sumBy(ZERO_LENGTH_COLUMNNAME_ARRAY);
    }

    @Override
    @ConcurrentMethod
    default TOPS sumBy(String... groupByColumns) {
        return sumBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default TOPS sumBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.sum(), groupByColumns);
    }

    @Override
    @ConcurrentMethod
    default TOPS sumBy(Collection<String> groupByColumns) {
        return sumBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    // -------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS avgBy() {
        return avgBy(ZERO_LENGTH_COLUMNNAME_ARRAY);
    }

    @Override
    @ConcurrentMethod
    default TOPS avgBy(String... groupByColumns) {
        return avgBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default TOPS avgBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.avg(), groupByColumns);
    }

    @Override
    @ConcurrentMethod
    default TOPS avgBy(Collection<String> groupByColumns) {
        return avgBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    // -------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS medianBy() {
        return medianBy(ZERO_LENGTH_COLUMNNAME_ARRAY);
    }

    @Override
    @ConcurrentMethod
    default TOPS medianBy(String... groupByColumns) {
        return medianBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default TOPS medianBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.median(), groupByColumns);
    }

    @Override
    @ConcurrentMethod
    default TOPS medianBy(Collection<String> groupByColumns) {
        return medianBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    // -------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS stdBy() {
        return stdBy(ZERO_LENGTH_COLUMNNAME_ARRAY);
    }

    @Override
    @ConcurrentMethod
    default TOPS stdBy(String... groupByColumns) {
        return stdBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default TOPS stdBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.std(), groupByColumns);
    }

    @Override
    @ConcurrentMethod
    default TOPS stdBy(Collection<String> groupByColumns) {
        return stdBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    // -------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS varBy() {
        return varBy(ZERO_LENGTH_COLUMNNAME_ARRAY);
    }

    @Override
    @ConcurrentMethod
    default TOPS varBy(String... groupByColumns) {
        return varBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default TOPS varBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.var(), groupByColumns);
    }

    @Override
    @ConcurrentMethod
    default TOPS varBy(Collection<String> groupByColumns) {
        return varBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    // -------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS absSumBy() {
        return absSumBy(ZERO_LENGTH_COLUMNNAME_ARRAY);
    }

    @Override
    @ConcurrentMethod
    default TOPS absSumBy(String... groupByColumns) {
        return absSumBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default TOPS absSumBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.absSum(), groupByColumns);
    }

    @Override
    @ConcurrentMethod
    default TOPS absSumBy(Collection<String> groupByColumns) {
        return absSumBy(ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    // -------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS wsumBy(String weightColumn) {
        return wsumBy(weightColumn, ZERO_LENGTH_COLUMNNAME_ARRAY);
    }

    @Override
    @ConcurrentMethod
    default TOPS wsumBy(String weightColumn, String... groupByColumns) {
        return wsumBy(weightColumn, ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default TOPS wsumBy(String weightColumn, ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.wsum(weightColumn), groupByColumns);
    }

    @Override
    @ConcurrentMethod
    default TOPS wsumBy(String weightColumn, Collection<String> groupByColumns) {
        return wsumBy(weightColumn, ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    // -------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS wavgBy(String weightColumn) {
        return wavgBy(weightColumn, ZERO_LENGTH_COLUMNNAME_ARRAY);
    }

    @Override
    @ConcurrentMethod
    default TOPS wavgBy(String weightColumn, String... groupByColumns) {
        return wavgBy(weightColumn, ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default TOPS wavgBy(String weightColumn, ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.wavg(weightColumn), groupByColumns);
    }

    @Override
    @ConcurrentMethod
    default TOPS wavgBy(String weightColumn, Collection<String> groupByColumns) {
        return wavgBy(weightColumn, ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    // -------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default TOPS dropColumns(Collection<String> columnNames) {
        return dropColumns(columnNames.toArray(new String[0]));
    }

    @Override
    @ConcurrentMethod
    default TOPS dropColumns(ColumnName... columnNames) {
        return dropColumns(Arrays.stream(columnNames).map(ColumnName::name).toArray(String[]::new));
    }

    // -------------------------------------------------------------------------------------------

    static Collection<String> splitToCollection(String string) {
        return string.trim().isEmpty() ? Collections.emptyList()
                : Arrays.stream(string.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                        .collect(Collectors.toList());
    }
}
