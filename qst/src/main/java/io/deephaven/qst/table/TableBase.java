/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.api.AsOfJoinRule;
import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.ReverseAsOfJoinRule;
import io.deephaven.api.Selectable;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.qst.TableCreationLogic;

import java.util.Collection;
import java.util.Optional;

public abstract class TableBase implements TableSpec {

    // Note: method implementations should use the static constructors, or builder patterns, for the
    // necessary TableSpec instead of delegating to other methods. The default values are the
    // responsibility of the TableSpec.

    @Override
    public final TableCreationLogic logic() {
        return new TableCreationLogicImpl(this);
    }

    @Override
    public final HeadTable head(long size) {
        return HeadTable.of(this, size);
    }

    @Override
    public final TailTable tail(long size) {
        return TailTable.of(this, size);
    }

    @Override
    public final ReverseTable reverse() {
        return ReverseTable.of(this);
    }

    @Override
    public final SnapshotTable snapshot(TableSpec baseTable, boolean doInitialSnapshot,
            String... stampColumns) {
        SnapshotTable.Builder builder = SnapshotTable.builder().trigger(this).base(baseTable)
                .doInitialSnapshot(doInitialSnapshot);
        for (String stampColumn : stampColumns) {
            builder.addStampColumns(ColumnName.of(stampColumn));
        }
        return builder.build();
    }

    @Override
    public final SortTable sort(Collection<SortColumn> columnsToSortBy) {
        return SortTable.builder().parent(this).addAllColumns(columnsToSortBy).build();
    }

    @Override
    public final WhereTable where(Collection<? extends Filter> filters) {
        return WhereTable.builder().parent(this).addAllFilters(filters).build();
    }

    @Override
    public final WhereInTable whereIn(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch) {
        return WhereInTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .build();
    }

    @Override
    public final WhereNotInTable whereNotIn(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch) {
        return WhereNotInTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .build();
    }

    @Override
    public final NaturalJoinTable naturalJoin(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return NaturalJoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .addAllAdditions(columnsToAdd).build();
    }

    @Override
    public final ExactJoinTable exactJoin(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return ExactJoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .addAllAdditions(columnsToAdd).build();
    }

    @Override
    public JoinTable join(TableSpec rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return JoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .addAllAdditions(columnsToAdd).build();
    }

    @Override
    public final JoinTable join(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, int reserveBits) {
        return JoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .addAllAdditions(columnsToAdd).reserveBits(reserveBits).build();
    }

    @Override
    public final AsOfJoinTable aj(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, AsOfJoinRule asOfJoinRule) {
        return AsOfJoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .addAllAdditions(columnsToAdd).rule(asOfJoinRule).build();
    }

    @Override
    public final ReverseAsOfJoinTable raj(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, ReverseAsOfJoinRule reverseAsOfJoinRule) {
        return ReverseAsOfJoinTable.builder().left(this).right(rightTable)
                .addAllMatches(columnsToMatch).addAllAdditions(columnsToAdd).rule(reverseAsOfJoinRule)
                .build();
    }

    @Override
    public final ViewTable view(Collection<? extends Selectable> columns) {
        return ViewTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final UpdateViewTable updateView(Collection<? extends Selectable> columns) {
        return UpdateViewTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final UpdateTable update(Collection<? extends Selectable> columns) {
        return UpdateTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final LazyUpdateTable lazyUpdate(Collection<? extends Selectable> columns) {
        return LazyUpdateTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final SelectTable select(Collection<? extends Selectable> columns) {
        return SelectTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final AggregateAllByTable aggAllBy(AggSpec spec, ColumnName... groupByColumns) {
        return AggregateAllByTable.builder().parent(this).spec(spec).addGroupByColumns(groupByColumns).build();
    }

    @Override
    public TableSpec aggBy(Collection<? extends Aggregation> aggregations, boolean preserveEmpty,
                           TableSpec initialGroups, Collection<? extends ColumnName> groupByColumns) {
        return AggregationTable.builder().parent(this)
                .addAllGroupByColumns(groupByColumns)
                .addAllAggregations(aggregations)
                .preserveEmpty(preserveEmpty)
                .initialGroups(Optional.ofNullable(initialGroups))
                .build();
    }

    @Override
    public final UpdateByTable updateBy(UpdateByControl control, Collection<? extends UpdateByOperation> operations,
            Collection<? extends ColumnName> byColumns) {
        return UpdateByTable.builder()
                .parent(this)
                .control(control)
                .addAllOperations(operations)
                .addAllGroupByColumns(byColumns)
                .build();
    }

    @Override
    public final SelectDistinctTable selectDistinct() {
        return SelectDistinctTable.builder().parent(this).build();
    }

    @Override
    public final SelectDistinctTable selectDistinct(Collection<? extends Selectable> columns) {
        return SelectDistinctTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final CountByTable countBy(String countColumnName, ColumnName... groupByColumns) {
        return CountByTable.builder().parent(this).countName(ColumnName.of(countColumnName))
                .addGroupByColumns(groupByColumns).build();
    }

    @Override
    public final <V extends TableSchema.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        // the auto-generated toString methods aren't very useful; and being recursive, they can
        // cause stack overflow exceptions that hide other errors in unit tests
        return super.toString();
    }
}
