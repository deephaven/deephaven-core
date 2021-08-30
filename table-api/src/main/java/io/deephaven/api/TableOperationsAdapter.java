package io.deephaven.api;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.filter.Filter;

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
    public final TOPS_1 leftJoin(TABLE_1 rightTable, String columnsToMatch) {
        return adapt(delegate.leftJoin(adapt(rightTable), columnsToMatch));
    }

    @Override
    public final TOPS_1 leftJoin(TABLE_1 rightTable, String columnsToMatch, String columnsToAdd) {
        return adapt(delegate.leftJoin(adapt(rightTable), columnsToMatch, columnsToAdd));
    }

    @Override
    public final TOPS_1 leftJoin(TABLE_1 rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return adapt(delegate.leftJoin(adapt(rightTable), columnsToMatch, columnsToAdd));
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
    public final TOPS_1 by() {
        return adapt(delegate.by());
    }

    @Override
    public final TOPS_1 by(String... groupByColumns) {
        return adapt(delegate.by(groupByColumns));
    }

    @Override
    public final TOPS_1 by(Collection<? extends Selectable> groupByColumns) {
        return adapt(delegate.by(groupByColumns));
    }

    @Override
    public final TOPS_1 by(Collection<? extends Selectable> groupByColumns,
            Collection<? extends Aggregation> aggregations) {
        return adapt(delegate.by(groupByColumns, aggregations));
    }
}
