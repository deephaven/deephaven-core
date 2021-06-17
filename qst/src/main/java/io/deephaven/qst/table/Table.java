package io.deephaven.qst.table;

import io.deephaven.api.Filter;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.Selectable;
import io.deephaven.api.SortColumn;
import io.deephaven.api.TableOperations;
import io.deephaven.api.agg.Aggregation;
import java.util.Collection;
import java.util.List;

public interface Table extends TableOperations<Table, Table> {

    static EmptyTable empty(long size) {
        return EmptyTable.of(size);
    }

    @Override
    HeadTable head(long size);

    @Override
    TailTable tail(long size);

    @Override
    ReverseTable reverse();

    @Override
    SortTable sort(String... columnsToSortBy);

    @Override
    SortTable sort(List<String> columnsToSortBy);

    @Override
    SortTable sortDescending(String... columnsToSortBy);

    @Override
    SortTable sortDescending(List<String> columnsToSortBy);

    @Override
    SortTable sort2(List<SortColumn> columnsToSortBy);

    @Override
    WhereTable where(String... filters);

    @Override
    WhereTable where(Collection<String> filters);

    @Override
    WhereTable where2(Collection<Filter> filters);

    @Override
    WhereInTable whereIn(Table rightTable, String... columnsToMatch);

    @Override
    WhereInTable whereIn(Table rightTable, Collection<String> columnsToMatch);

    @Override
    WhereInTable whereIn2(Table rightTable, Collection<JoinMatch> columnsToMatch);

    @Override
    WhereNotInTable whereNotIn(Table rightTable, String... columnsToMatch);

    @Override
    WhereNotInTable whereNotIn(Table rightTable, Collection<String> columnsToMatch);

    @Override
    WhereNotInTable whereNotIn2(Table rightTable, Collection<JoinMatch> columnsToMatch);

    @Override
    NaturalJoinTable naturalJoin2(Table rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd);

    @Override
    NaturalJoinTable naturalJoin(Table rightTable, Collection<String> columnsToMatch,
        Collection<String> columnsToAdd);

    @Override
    NaturalJoinTable naturalJoin(Table rightTable, Collection<String> columnsToMatch);

    @Override
    NaturalJoinTable naturalJoin(Table rightTable, String columnsToMatch);

    @Override
    NaturalJoinTable naturalJoin(Table rightTable, String columnsToMatch, String columnsToAdd);

    @Override
    ExactJoinTable exactJoin2(Table rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd);

    @Override
    ExactJoinTable exactJoin(Table rightTable, Collection<String> columnsToMatch,
        Collection<String> columnsToAdd);

    @Override
    ExactJoinTable exactJoin(Table rightTable, Collection<String> columnsToMatch);

    @Override
    ExactJoinTable exactJoin(Table rightTable, String columnsToMatch);

    @Override
    ExactJoinTable exactJoin(Table rightTable, String columnsToMatch, String columnsToAdd);

    @Override
    JoinTable join2(Table rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd);

    @Override
    JoinTable join(Table rightTable, Collection<String> columnsToMatch,
        Collection<String> columnsToAdd);

    @Override
    JoinTable join(Table rightTable, String columnsToMatch);

    @Override
    JoinTable join(Table rightTable, String columnsToMatch, String columnsToAdd);

    @Override
    ViewTable view(String... columns);

    @Override
    ViewTable view(Collection<String> columns);

    @Override
    ViewTable view2(Collection<Selectable> columns);

    @Override
    UpdateViewTable updateView(String... columns);

    @Override
    UpdateViewTable updateView(Collection<String> columns);

    @Override
    UpdateViewTable updateView2(Collection<Selectable> columns);

    @Override
    UpdateTable update(String... columns);

    @Override
    UpdateTable update(Collection<String> columns);

    @Override
    UpdateTable update2(Collection<Selectable> columns);

    @Override
    SelectTable select();

    @Override
    SelectTable select(String... columns);

    @Override
    SelectTable select(Collection<String> columns);

    @Override
    SelectTable select2(Collection<Selectable> columns);

    @Override
    ByTable by();

    @Override
    ByTable by(String... groupByColumns);

    @Override
    ByTable by(Collection<String> groupByColumns);

    @Override
    ByTable by2(Collection<Selectable> groupByColumns);

    @Override
    AggregationTable by(Collection<Selectable> groupByColumns,
        Collection<Aggregation> aggregations);

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(EmptyTable emptyTable);

        void visit(NewTable newTable);

        void visit(QueryScopeTable queryScopeTable);

        void visit(HeadTable headTable);

        void visit(TailTable tailTable);

        void visit(ReverseTable reverseTable);

        void visit(SortTable sortTable);

        void visit(WhereTable whereTable);

        void visit(WhereInTable whereInTable);

        void visit(WhereNotInTable whereNotInTable);

        void visit(NaturalJoinTable naturalJoinTable);

        void visit(ExactJoinTable exactJoinTable);

        void visit(JoinTable joinTable);

        void visit(ViewTable viewTable);

        void visit(UpdateViewTable updateViewTable);

        void visit(UpdateTable updateTable);

        void visit(SelectTable selectTable);

        void visit(ByTable byTable);

        void visit(AggregationTable aggregationTable);
    }
}
