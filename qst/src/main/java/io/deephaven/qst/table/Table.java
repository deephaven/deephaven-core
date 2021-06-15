package io.deephaven.qst.table;

import io.deephaven.qst.TableOperations;
import java.util.Collection;

public interface Table extends TableOperations<Table, Table> {

    static EmptyTable empty(long size) {
        return EmptyTable.of(size);
    }

    static EmptyTable empty(long size, TableHeader header) {
        return EmptyTable.of(size, header);
    }

    @Override
    HeadTable head(long size);

    @Override
    TailTable tail(long size);

    @Override
    WhereTable where(String... filters);

    @Override
    WhereTable where(Collection<String> filters);

    @Override
    WhereTable where2(Collection<Filter> filters);

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

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(EmptyTable emptyTable);

        void visit(NewTable newTable);

        void visit(HeadTable headTable);

        void visit(TailTable tailTable);

        void visit(WhereTable whereTable);

        void visit(NaturalJoinTable naturalJoinTable);

        void visit(ExactJoinTable exactJoinTable);

        void visit(ViewTable viewTable);

        void visit(UpdateViewTable updateViewTable);

        void visit(UpdateTable updateTable);

        void visit(SelectTable selectTable);
    }
}
