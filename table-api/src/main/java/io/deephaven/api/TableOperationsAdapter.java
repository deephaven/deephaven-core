/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.api.updateby.UpdateByControl;

import java.util.Collection;
import java.util.Objects;

public abstract class TableOperationsAdapter<TOPS_1 extends TableOperations<TOPS_1, TABLE_1>, TABLE_1, TOPS_2 extends TableOperations<TOPS_2, TABLE_2>, TABLE_2>
        implements TableOperations<TOPS_1, TABLE_1> {

    private final TOPS_2 delegate;

    public TableOperationsAdapter(TOPS_2 delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    protected abstract TOPS_1 adapt(TOPS_2 ops);

    protected abstract TABLE_2 adapt(TABLE_1 rhs);

    public final TOPS_2 delegate() {
        return delegate;
    }

    @Override
    public final TOPS_1 head(long size) {
        return adapt(delegate.head(size));
    }

    @Override
    public final TOPS_1 tail(long size) {
        return adapt(delegate.tail(size));
    }

    @Override
    public final TOPS_1 reverse() {
        return adapt(delegate.reverse());
    }

    @Override
    public final TOPS_1 snapshot(TABLE_1 baseTable, String... stampColumns) {
        return adapt(delegate.snapshot(adapt(baseTable), stampColumns));
    }

    @Override
    public final TOPS_1 snapshot(TABLE_1 baseTable, boolean doInitialSnapshot,
            String... stampColumns) {
        return adapt(delegate.snapshot(adapt(baseTable), doInitialSnapshot, stampColumns));
    }

    @Override
    public final TOPS_1 snapshot(TABLE_1 baseTable, boolean doInitialSnapshot,
            Collection<ColumnName> stampColumns) {
        return adapt(delegate.snapshot(adapt(baseTable), doInitialSnapshot, stampColumns));
    }

    @Override
    public final TOPS_1 sort(String... columnsToSortBy) {
        return adapt(delegate.sort(columnsToSortBy));
    }

    @Override
    public final TOPS_1 sortDescending(String... columnsToSortBy) {
        return adapt(delegate.sortDescending(columnsToSortBy));
    }

    @Override
    public final TOPS_1 sort(Collection<SortColumn> columnsToSortBy) {
        return adapt(delegate.sort(columnsToSortBy));
    }

    @Override
    public final TOPS_1 where(String... filters) {
        return adapt(delegate.where(filters));
    }

    @Override
    public final TOPS_1 where(Collection<? extends Filter> filters) {
        return adapt(delegate.where(filters));
    }

    @Override
    public final TOPS_1 whereIn(TABLE_1 rightTable, String... columnsToMatch) {
        return adapt(delegate.whereIn(adapt(rightTable), columnsToMatch));
    }

    @Override
    public final TOPS_1 whereIn(TABLE_1 rightTable,
            Collection<? extends JoinMatch> columnsToMatch) {
        return adapt(delegate.whereIn(adapt(rightTable), columnsToMatch));
    }

    @Override
    public final TOPS_1 whereNotIn(TABLE_1 rightTable, String... columnsToMatch) {
        return adapt(delegate.whereNotIn(adapt(rightTable), columnsToMatch));
    }

    @Override
    public final TOPS_1 whereNotIn(TABLE_1 rightTable,
            Collection<? extends JoinMatch> columnsToMatch) {
        return adapt(delegate.whereNotIn(adapt(rightTable), columnsToMatch));
    }

    @Override
    public final TOPS_1 view(String... columns) {
        return adapt(delegate.view(columns));
    }

    @Override
    public final TOPS_1 view(Collection<? extends Selectable> columns) {
        return adapt(delegate.view(columns));
    }

    @Override
    public final TOPS_1 updateView(String... columns) {
        return adapt(delegate.updateView(columns));
    }

    @Override
    public final TOPS_1 updateView(Collection<? extends Selectable> columns) {
        return adapt(delegate.updateView(columns));
    }

    @Override
    public final TOPS_1 update(String... columns) {
        return adapt(delegate.update(columns));
    }

    @Override
    public final TOPS_1 update(Collection<? extends Selectable> columns) {
        return adapt(delegate.update(columns));
    }

    @Override
    public final TOPS_1 lazyUpdate(String... columns) {
        return adapt(delegate.lazyUpdate(columns));
    }

    @Override
    public final TOPS_1 lazyUpdate(Collection<? extends Selectable> columns) {
        return adapt(delegate.lazyUpdate(columns));
    }

    @Override
    public final TOPS_1 select(String... columns) {
        return adapt(delegate.select(columns));
    }

    @Override
    public final TOPS_1 select(Collection<? extends Selectable> columns) {
        return adapt(delegate.select(columns));
    }

    @Override
    public final TOPS_1 naturalJoin(TABLE_1 rightTable, String columnsToMatch) {
        return adapt(delegate.naturalJoin(adapt(rightTable), columnsToMatch));
    }

    @Override
    public final TOPS_1 naturalJoin(TABLE_1 rightTable, String columnsToMatch,
            String columnsToAdd) {
        return adapt(delegate.naturalJoin(adapt(rightTable), columnsToMatch, columnsToAdd));
    }

    @Override
    public final TOPS_1 naturalJoin(TABLE_1 rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return adapt(delegate.naturalJoin(adapt(rightTable), columnsToMatch, columnsToAdd));
    }

    @Override
    public final TOPS_1 exactJoin(TABLE_1 rightTable, String columnsToMatch) {
        return adapt(delegate.exactJoin(adapt(rightTable), columnsToMatch));
    }

    @Override
    public final TOPS_1 exactJoin(TABLE_1 rightTable, String columnsToMatch, String columnsToAdd) {
        return adapt(delegate.exactJoin(adapt(rightTable), columnsToMatch, columnsToAdd));
    }

    @Override
    public final TOPS_1 exactJoin(TABLE_1 rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return adapt(delegate.exactJoin(adapt(rightTable), columnsToMatch, columnsToAdd));
    }

    @Override
    public final TOPS_1 join(TABLE_1 rightTable, String columnsToMatch) {
        return adapt(delegate.join(adapt(rightTable), columnsToMatch));
    }

    @Override
    public final TOPS_1 join(TABLE_1 rightTable, String columnsToMatch, String columnsToAdd) {
        return adapt(delegate.join(adapt(rightTable), columnsToMatch, columnsToAdd));
    }

    @Override
    public final TOPS_1 join(TABLE_1 rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return adapt(delegate.join(adapt(rightTable), columnsToMatch, columnsToAdd));
    }

    @Override
    public final TOPS_1 join(TABLE_1 rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, int reserveBits) {
        return adapt(delegate.join(adapt(rightTable), columnsToMatch, columnsToAdd, reserveBits));
    }

    @Override
    public final TOPS_1 aj(TABLE_1 rightTable, String columnsToMatch) {
        return adapt(delegate.aj(adapt(rightTable), columnsToMatch));
    }

    @Override
    public final TOPS_1 aj(TABLE_1 rightTable, String columnsToMatch, String columnsToAdd) {
        return adapt(delegate.aj(adapt(rightTable), columnsToMatch, columnsToAdd));
    }

    @Override
    public final TOPS_1 aj(TABLE_1 rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return adapt(delegate.aj(adapt(rightTable), columnsToMatch, columnsToAdd));
    }

    @Override
    public final TOPS_1 aj(TABLE_1 rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, AsOfJoinRule asOfJoinRule) {
        return adapt(delegate.aj(adapt(rightTable), columnsToMatch, columnsToAdd, asOfJoinRule));
    }

    @Override
    public final TOPS_1 raj(TABLE_1 rightTable, String columnsToMatch) {
        return adapt(delegate.raj(adapt(rightTable), columnsToMatch));
    }

    @Override
    public final TOPS_1 raj(TABLE_1 rightTable, String columnsToMatch, String columnsToAdd) {
        return adapt(delegate.raj(adapt(rightTable), columnsToMatch, columnsToAdd));
    }

    @Override
    public final TOPS_1 raj(TABLE_1 rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return adapt(delegate.raj(adapt(rightTable), columnsToMatch, columnsToAdd));
    }

    @Override
    public final TOPS_1 raj(TABLE_1 rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, ReverseAsOfJoinRule reverseAsOfJoinRule) {
        return adapt(
                delegate.raj(adapt(rightTable), columnsToMatch, columnsToAdd, reverseAsOfJoinRule));
    }

    @Override
    public final TOPS_1 groupBy() {
        return adapt(delegate.groupBy());
    }

    @Override
    public final TOPS_1 groupBy(String... groupByColumns) {
        return adapt(delegate.groupBy(groupByColumns));
    }

    @Override
    public final TOPS_1 groupBy(Collection<? extends ColumnName> groupByColumns) {
        return adapt(delegate.groupBy(groupByColumns));
    }

    @Override
    public final TOPS_1 aggAllBy(AggSpec spec) {
        return adapt(delegate.aggAllBy(spec));
    }

    @Override
    public final TOPS_1 aggAllBy(AggSpec spec, String... groupByColumns) {
        return adapt(delegate.aggAllBy(spec, groupByColumns));
    }

    @Override
    public final TOPS_1 aggAllBy(AggSpec spec, ColumnName... groupByColumns) {
        return adapt(delegate.aggAllBy(spec, groupByColumns));
    }

    @Override
    public final TOPS_1 aggAllBy(AggSpec spec, Collection<String> groupByColumns) {
        return adapt(delegate.aggAllBy(spec, groupByColumns));
    }

    @Override
    public final TOPS_1 aggBy(Aggregation aggregation) {
        return adapt(delegate.aggBy(aggregation));
    }

    @Override
    public final TOPS_1 aggBy(Collection<? extends Aggregation> aggregations) {
        return adapt(delegate.aggBy(aggregations));
    }

    @Override
    public TOPS_1 aggBy(Collection<? extends Aggregation> aggregations, boolean preserveEmpty) {
        return adapt(delegate.aggBy(aggregations, preserveEmpty));
    }

    @Override
    public final TOPS_1 aggBy(Aggregation aggregation, String... groupByColumns) {
        return adapt(delegate.aggBy(aggregation, groupByColumns));
    }

    @Override
    public final TOPS_1 aggBy(Aggregation aggregation, Collection<? extends ColumnName> groupByColumns) {
        return adapt(delegate.aggBy(aggregation, groupByColumns));
    }

    @Override
    public final TOPS_1 aggBy(Collection<? extends Aggregation> aggregations, String... groupByColumns) {
        return adapt(delegate.aggBy(aggregations, groupByColumns));
    }

    @Override
    public final TOPS_1 aggBy(Collection<? extends Aggregation> aggregations,
            Collection<? extends ColumnName> groupByColumns) {
        return adapt(delegate.aggBy(aggregations, groupByColumns));
    }


    @Override
    public final TOPS_1 updateBy(UpdateByOperation operation) {
        return adapt(delegate.updateBy(operation));
    }

    @Override
    public final TOPS_1 updateBy(UpdateByOperation operation, String... byColumns) {
        return adapt(delegate.updateBy(operation, byColumns));
    }

    @Override
    public final TOPS_1 updateBy(Collection<? extends UpdateByOperation> operations) {
        return adapt(delegate.updateBy(operations));
    }

    @Override
    public final TOPS_1 updateBy(Collection<? extends UpdateByOperation> operations, String... byColumns) {
        return adapt(delegate.updateBy(operations, byColumns));
    }

    @Override
    public final TOPS_1 updateBy(Collection<? extends UpdateByOperation> operations,
            Collection<? extends ColumnName> byColumns) {
        return adapt(delegate.updateBy(operations, byColumns));
    }

    @Override
    public final TOPS_1 updateBy(UpdateByControl control, Collection<? extends UpdateByOperation> operations) {
        return adapt(delegate.updateBy(control, operations));
    }

    @Override
    public final TOPS_1 updateBy(UpdateByControl control, Collection<? extends UpdateByOperation> operations,
            Collection<? extends ColumnName> byColumns) {
        return adapt(delegate.updateBy(control, operations, byColumns));
    }

    @Override
    public TOPS_1 aggBy(Collection<? extends Aggregation> aggregations, boolean preserveEmpty, TABLE_1 initialGroups,
            Collection<? extends ColumnName> groupByColumns) {
        return adapt(delegate.aggBy(aggregations, preserveEmpty, initialGroups == null ? null : adapt(initialGroups),
                groupByColumns));
    }

    @Override
    public final TOPS_1 selectDistinct() {
        return adapt(delegate.selectDistinct());
    }

    @Override
    public final TOPS_1 selectDistinct(String... columns) {
        return adapt(delegate.selectDistinct(columns));
    }

    @Override
    public final TOPS_1 selectDistinct(Selectable... columns) {
        return adapt(delegate.selectDistinct(columns));
    }

    @Override
    public final TOPS_1 selectDistinct(Collection<? extends Selectable> columns) {
        return adapt(delegate.selectDistinct(columns));
    }

    @Override
    public final TOPS_1 countBy(String countColumnName) {
        return adapt(delegate.countBy(countColumnName));
    }

    @Override
    public final TOPS_1 countBy(String countColumnName, String... groupByColumns) {
        return adapt(delegate.countBy(countColumnName, groupByColumns));
    }

    @Override
    public final TOPS_1 countBy(String countColumnName, ColumnName... groupByColumns) {
        return adapt(delegate.countBy(countColumnName, groupByColumns));
    }

    @Override
    public final TOPS_1 countBy(String countColumnName, Collection<String> groupByColumns) {
        return adapt(delegate.countBy(countColumnName, groupByColumns));
    }

    @Override
    public final TOPS_1 firstBy() {
        return adapt(delegate.firstBy());
    }

    @Override
    public final TOPS_1 firstBy(String... groupByColumns) {
        return adapt(delegate.firstBy(groupByColumns));
    }

    @Override
    public final TOPS_1 firstBy(ColumnName... groupByColumns) {
        return adapt(delegate.firstBy(groupByColumns));
    }

    @Override
    public final TOPS_1 firstBy(Collection<String> groupByColumns) {
        return adapt(delegate.firstBy(groupByColumns));
    }

    @Override
    public final TOPS_1 lastBy() {
        return adapt(delegate.lastBy());
    }

    @Override
    public final TOPS_1 lastBy(String... groupByColumns) {
        return adapt(delegate.lastBy(groupByColumns));
    }

    @Override
    public final TOPS_1 lastBy(ColumnName... groupByColumns) {
        return adapt(delegate.lastBy(groupByColumns));
    }

    @Override
    public final TOPS_1 lastBy(Collection<String> groupByColumns) {
        return adapt(delegate.lastBy(groupByColumns));
    }

    @Override
    public final TOPS_1 minBy() {
        return adapt(delegate.minBy());
    }

    @Override
    public final TOPS_1 minBy(String... groupByColumns) {
        return adapt(delegate.minBy(groupByColumns));
    }

    @Override
    public final TOPS_1 minBy(ColumnName... groupByColumns) {
        return adapt(delegate.minBy(groupByColumns));
    }

    @Override
    public final TOPS_1 minBy(Collection<String> groupByColumns) {
        return adapt(delegate.minBy(groupByColumns));
    }

    @Override
    public final TOPS_1 maxBy() {
        return adapt(delegate.maxBy());
    }

    @Override
    public final TOPS_1 maxBy(String... groupByColumns) {
        return adapt(delegate.maxBy(groupByColumns));
    }

    @Override
    public final TOPS_1 maxBy(ColumnName... groupByColumns) {
        return adapt(delegate.maxBy(groupByColumns));
    }

    @Override
    public final TOPS_1 maxBy(Collection<String> groupByColumns) {
        return adapt(delegate.maxBy(groupByColumns));
    }

    @Override
    public final TOPS_1 sumBy() {
        return adapt(delegate.sumBy());
    }

    @Override
    public final TOPS_1 sumBy(String... groupByColumns) {
        return adapt(delegate.sumBy(groupByColumns));
    }

    @Override
    public final TOPS_1 sumBy(ColumnName... groupByColumns) {
        return adapt(delegate.sumBy(groupByColumns));
    }

    @Override
    public final TOPS_1 sumBy(Collection<String> groupByColumns) {
        return adapt(delegate.sumBy(groupByColumns));
    }

    @Override
    public final TOPS_1 avgBy() {
        return adapt(delegate.avgBy());
    }

    @Override
    public final TOPS_1 avgBy(String... groupByColumns) {
        return adapt(delegate.avgBy(groupByColumns));
    }

    @Override
    public final TOPS_1 avgBy(ColumnName... groupByColumns) {
        return adapt(delegate.avgBy(groupByColumns));
    }

    @Override
    public final TOPS_1 avgBy(Collection<String> groupByColumns) {
        return adapt(delegate.avgBy(groupByColumns));
    }

    @Override
    public final TOPS_1 medianBy() {
        return adapt(delegate.medianBy());
    }

    @Override
    public final TOPS_1 medianBy(String... groupByColumns) {
        return adapt(delegate.medianBy(groupByColumns));
    }

    @Override
    public final TOPS_1 medianBy(ColumnName... groupByColumns) {
        return adapt(delegate.medianBy(groupByColumns));
    }

    @Override
    public final TOPS_1 medianBy(Collection<String> groupByColumns) {
        return adapt(delegate.medianBy(groupByColumns));
    }

    @Override
    public final TOPS_1 stdBy() {
        return adapt(delegate.stdBy());
    }

    @Override
    public final TOPS_1 stdBy(String... groupByColumns) {
        return adapt(delegate.stdBy(groupByColumns));
    }

    @Override
    public final TOPS_1 stdBy(ColumnName... groupByColumns) {
        return adapt(delegate.stdBy(groupByColumns));
    }

    @Override
    public final TOPS_1 stdBy(Collection<String> groupByColumns) {
        return adapt(delegate.stdBy(groupByColumns));
    }

    @Override
    public final TOPS_1 varBy() {
        return adapt(delegate.varBy());
    }

    @Override
    public final TOPS_1 varBy(String... groupByColumns) {
        return adapt(delegate.varBy(groupByColumns));
    }

    @Override
    public final TOPS_1 varBy(ColumnName... groupByColumns) {
        return adapt(delegate.varBy(groupByColumns));
    }

    @Override
    public final TOPS_1 varBy(Collection<String> groupByColumns) {
        return adapt(delegate.varBy(groupByColumns));
    }

    @Override
    public final TOPS_1 absSumBy() {
        return adapt(delegate.absSumBy());
    }

    @Override
    public final TOPS_1 absSumBy(String... groupByColumns) {
        return adapt(delegate.absSumBy(groupByColumns));
    }

    @Override
    public final TOPS_1 absSumBy(ColumnName... groupByColumns) {
        return adapt(delegate.absSumBy(groupByColumns));
    }

    @Override
    public final TOPS_1 absSumBy(Collection<String> groupByColumns) {
        return adapt(delegate.absSumBy(groupByColumns));
    }

    @Override
    public final TOPS_1 wsumBy(String weightColumn) {
        return adapt(delegate.wsumBy(weightColumn));
    }

    @Override
    public final TOPS_1 wsumBy(String weightColumn, String... groupByColumns) {
        return adapt(delegate.wsumBy(weightColumn, groupByColumns));
    }

    @Override
    public final TOPS_1 wsumBy(String weightColumn, ColumnName... groupByColumns) {
        return adapt(delegate.wsumBy(weightColumn, groupByColumns));
    }

    @Override
    public final TOPS_1 wsumBy(String weightColumn, Collection<String> groupByColumns) {
        return adapt(delegate.wsumBy(weightColumn, groupByColumns));
    }

    @Override
    public final TOPS_1 wavgBy(String weightColumn) {
        return adapt(delegate.wavgBy(weightColumn));
    }

    @Override
    public final TOPS_1 wavgBy(String weightColumn, String... groupByColumns) {
        return adapt(delegate.wavgBy(weightColumn, groupByColumns));
    }

    @Override
    public final TOPS_1 wavgBy(String weightColumn, ColumnName... groupByColumns) {
        return adapt(delegate.wavgBy(weightColumn, groupByColumns));
    }

    @Override
    public final TOPS_1 wavgBy(String weightColumn, Collection<String> groupByColumns) {
        return adapt(delegate.wavgBy(weightColumn, groupByColumns));
    }
}
