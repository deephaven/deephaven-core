package io.deephaven.qst.table;

import io.deephaven.api.AsOfJoinRule;
import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.ReverseAsOfJoinRule;
import io.deephaven.api.Selectable;
import io.deephaven.api.SortColumn;
import io.deephaven.api.TableOperations;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.filter.Filter;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

public interface Table extends TableOperations<Table, Table>, Serializable {

    static EmptyTable empty(long size) {
        return EmptyTable.of(size);
    }

    static MergeTable merge(Table... tables) {
        return MergeTable.of(tables);
    }

    static MergeTable merge(Collection<? extends Table> tables) {
        return MergeTable.of(tables);
    }

    static Table file(Path path) throws IOException, ClassNotFoundException {
        try (InputStream in = Files.newInputStream(path);
            BufferedInputStream buf = new BufferedInputStream(in);
            ObjectInputStream oIn = new ObjectInputStream(buf)) {
            return (Table) oIn.readObject();
        }
    }

    @Override
    HeadTable head(long size);

    @Override
    TailTable tail(long size);

    @Override
    ReverseTable reverse();

    @Override
    SnapshotTable snapshot(Table rightTable, String... stampColumns);

    @Override
    SnapshotTable snapshot(Table rightTable, boolean doInitialSnapshot, String... stampColumns);

    @Override
    SnapshotTable snapshot(Table rightTable, boolean doInitialSnapshot,
        Collection<ColumnName> stampColumns);

    @Override
    SortTable sort(String... columnsToSortBy);

    @Override
    SortTable sortDescending(String... columnsToSortBy);

    @Override
    SortTable sort(Collection<SortColumn> columnsToSortBy);

    @Override
    WhereTable where(String... filters);

    @Override
    WhereTable where(Collection<? extends Filter> filters);

    @Override
    WhereInTable whereIn(Table rightTable, String... columnsToMatch);

    @Override
    WhereInTable whereIn(Table rightTable, Collection<? extends JoinMatch> columnsToMatch);

    @Override
    WhereNotInTable whereNotIn(Table rightTable, String... columnsToMatch);

    @Override
    WhereNotInTable whereNotIn(Table rightTable, Collection<? extends JoinMatch> columnsToMatch);

    @Override
    NaturalJoinTable naturalJoin(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
        Collection<? extends JoinAddition> columnsToAdd);

    @Override
    NaturalJoinTable naturalJoin(Table rightTable, String columnsToMatch);

    @Override
    NaturalJoinTable naturalJoin(Table rightTable, String columnsToMatch, String columnsToAdd);

    @Override
    ExactJoinTable exactJoin(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
        Collection<? extends JoinAddition> columnsToAdd);

    @Override
    ExactJoinTable exactJoin(Table rightTable, String columnsToMatch);

    @Override
    ExactJoinTable exactJoin(Table rightTable, String columnsToMatch, String columnsToAdd);

    @Override
    JoinTable join(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
        Collection<? extends JoinAddition> columnsToAdd);

    @Override
    JoinTable join(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
        Collection<? extends JoinAddition> columnsToAdd, int reserveBits);

    @Override
    JoinTable join(Table rightTable, String columnsToMatch);

    @Override
    JoinTable join(Table rightTable, String columnsToMatch, String columnsToAdd);

    @Override
    Table leftJoin(Table rightTable, String columnsToMatch);

    @Override
    LeftJoinTable leftJoin(Table rightTable, String columnsToMatch, String columnsToAdd);

    @Override
    LeftJoinTable leftJoin(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
        Collection<? extends JoinAddition> columnsToAdd);

    @Override
    AsOfJoinTable aj(Table rightTable, String columnsToMatch);

    @Override
    AsOfJoinTable aj(Table rightTable, String columnsToMatch, String columnsToAdd);

    @Override
    AsOfJoinTable aj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
        Collection<? extends JoinAddition> columnsToAdd);

    @Override
    AsOfJoinTable aj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
        Collection<? extends JoinAddition> columnsToAdd, AsOfJoinRule asOfJoinRule);

    @Override
    ReverseAsOfJoinTable raj(Table rightTable, String columnsToMatch);

    @Override
    ReverseAsOfJoinTable raj(Table rightTable, String columnsToMatch, String columnsToAdd);

    @Override
    ReverseAsOfJoinTable raj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
        Collection<? extends JoinAddition> columnsToAdd);

    @Override
    ReverseAsOfJoinTable raj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
        Collection<? extends JoinAddition> columnsToAdd, ReverseAsOfJoinRule reverseAsOfJoinRule);

    @Override
    ViewTable view(String... columns);

    @Override
    ViewTable view(Collection<? extends Selectable> columns);

    @Override
    UpdateViewTable updateView(String... columns);

    @Override
    UpdateViewTable updateView(Collection<? extends Selectable> columns);

    @Override
    UpdateTable update(String... columns);

    @Override
    UpdateTable update(Collection<? extends Selectable> columns);

    @Override
    SelectTable select(String... columns);

    @Override
    SelectTable select(Collection<? extends Selectable> columns);

    @Override
    ByTable by();

    @Override
    ByTable by(String... groupByColumns);

    @Override
    ByTable by(Collection<? extends Selectable> groupByColumns);

    @Override
    AggregationTable by(Collection<? extends Selectable> groupByColumns,
        Collection<? extends Aggregation> aggregations);

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(EmptyTable emptyTable);

        void visit(NewTable newTable);

        void visit(TimeTable timeTable);

        void visit(MergeTable mergeTable);

        void visit(HeadTable headTable);

        void visit(TailTable tailTable);

        void visit(ReverseTable reverseTable);

        void visit(SortTable sortTable);

        void visit(SnapshotTable snapshotTable);

        void visit(WhereTable whereTable);

        void visit(WhereInTable whereInTable);

        void visit(WhereNotInTable whereNotInTable);

        void visit(NaturalJoinTable naturalJoinTable);

        void visit(ExactJoinTable exactJoinTable);

        void visit(JoinTable joinTable);

        void visit(LeftJoinTable leftJoinTable);

        void visit(AsOfJoinTable aj);

        void visit(ReverseAsOfJoinTable raj);

        void visit(ViewTable viewTable);

        void visit(UpdateViewTable updateViewTable);

        void visit(UpdateTable updateTable);

        void visit(SelectTable selectTable);

        void visit(ByTable byTable);

        void visit(AggregationTable aggregationTable);
    }
}
