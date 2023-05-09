/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.snapshot.SnapshotWhenOptions;
import io.deephaven.api.snapshot.SnapshotWhenOptions.Flag;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.api.updateby.UpdateByControl;

import java.util.Collection;
import java.util.Objects;

public abstract class TableOperationsAdapter<TOPS_1 extends TableOperations<TOPS_1, TABLE_1>, TABLE_1, TOPS_2 extends TableOperations<TOPS_2, TABLE_2>, TABLE_2>
        implements TableOperationsDefaults<TOPS_1, TABLE_1> {

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
    public final TOPS_1 snapshot() {
        return adapt(delegate.snapshot());
    }

    @Override
    public final TOPS_1 snapshotWhen(TABLE_1 trigger, Flag... features) {
        return adapt(delegate.snapshotWhen(adapt(trigger), features));
    }

    @Override
    public final TOPS_1 snapshotWhen(TABLE_1 trigger, Collection<Flag> features, String... stampColumns) {
        return adapt(delegate.snapshotWhen(adapt(trigger), features, stampColumns));
    }

    @Override
    public final TOPS_1 snapshotWhen(TABLE_1 trigger, SnapshotWhenOptions options) {
        return adapt(delegate.snapshotWhen(adapt(trigger), options));
    }

    @Override
    public final TOPS_1 sort(Collection<SortColumn> columnsToSortBy) {
        return adapt(delegate.sort(columnsToSortBy));
    }

    @Override
    public final TOPS_1 where(Collection<? extends Filter> filters) {
        return adapt(delegate.where(filters));
    }

    @Override
    public final TOPS_1 whereIn(TABLE_1 rightTable,
            Collection<? extends JoinMatch> columnsToMatch) {
        return adapt(delegate.whereIn(adapt(rightTable), columnsToMatch));
    }

    @Override
    public final TOPS_1 whereNotIn(TABLE_1 rightTable,
            Collection<? extends JoinMatch> columnsToMatch) {
        return adapt(delegate.whereNotIn(adapt(rightTable), columnsToMatch));
    }

    @Override
    public final TOPS_1 view(Collection<? extends Selectable> columns) {
        return adapt(delegate.view(columns));
    }

    @Override
    public final TOPS_1 updateView(Collection<? extends Selectable> columns) {
        return adapt(delegate.updateView(columns));
    }

    @Override
    public final TOPS_1 update(Collection<? extends Selectable> columns) {
        return adapt(delegate.update(columns));
    }

    @Override
    public final TOPS_1 lazyUpdate(Collection<? extends Selectable> columns) {
        return adapt(delegate.lazyUpdate(columns));
    }

    @Override
    public final TOPS_1 select(Collection<? extends Selectable> columns) {
        return adapt(delegate.select(columns));
    }

    @Override
    public final TOPS_1 naturalJoin(TABLE_1 rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return adapt(delegate.naturalJoin(adapt(rightTable), columnsToMatch, columnsToAdd));
    }

    @Override
    public final TOPS_1 exactJoin(TABLE_1 rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return adapt(delegate.exactJoin(adapt(rightTable), columnsToMatch, columnsToAdd));
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
    public final TOPS_1 aj(TABLE_1 rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, AsOfJoinRule asOfJoinRule) {
        return adapt(delegate.aj(adapt(rightTable), columnsToMatch, columnsToAdd, asOfJoinRule));
    }

    @Override
    public final TOPS_1 raj(TABLE_1 rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, ReverseAsOfJoinRule reverseAsOfJoinRule) {
        return adapt(
                delegate.raj(adapt(rightTable), columnsToMatch, columnsToAdd, reverseAsOfJoinRule));
    }

    @Override
    public TOPS_1 rangeJoin(TABLE_1 rightTable, Collection<? extends JoinMatch> exactMatches, RangeJoinMatch rangeMatch,
            Collection<? extends Aggregation> aggregations) {
        return adapt(delegate.rangeJoin(adapt(rightTable), exactMatches, rangeMatch, aggregations));
    }

    @Override
    public final TOPS_1 aggAllBy(AggSpec spec, ColumnName... groupByColumns) {
        return adapt(delegate.aggAllBy(spec, groupByColumns));
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
    public final TOPS_1 selectDistinct(Collection<? extends Selectable> columns) {
        return adapt(delegate.selectDistinct(columns));
    }

    @Override
    public final TOPS_1 ungroup(boolean nullFill, Collection<? extends ColumnName> columnsToUngroup) {
        return adapt(delegate.ungroup(nullFill, columnsToUngroup));
    }

    @Override
    public final TOPS_1 dropColumns(String... columnNames) {
        return adapt(delegate.dropColumns(columnNames));
    }
}
