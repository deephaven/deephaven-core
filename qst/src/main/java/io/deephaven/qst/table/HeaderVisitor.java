package io.deephaven.qst.table;

import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.Table.Visitor;

import java.util.Objects;

public class HeaderVisitor implements Visitor {

    public static TableHeader of(Table table) {
        return table.walk(new HeaderVisitor()).getOut();
    }

    private TableHeader out;

    public TableHeader getOut() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(EmptyTable emptyTable) {
        out = TableHeader.empty();
    }

    @Override
    public void visit(NewTable newTable) {
        out = newTable.header();
    }

    @Override
    public void visit(TimeTable timeTable) {
        out = TableHeader.of(ColumnHeader.ofLong("Timestamp"));
    }

    @Override
    public void visit(MergeTable mergeTable) {
        out = of(mergeTable.tables().get(0));
    }

    @Override
    public void visit(HeadTable headTable) {
        out = of(headTable.parent());
    }

    @Override
    public void visit(TailTable tailTable) {
        out = of(tailTable.parent());
    }

    @Override
    public void visit(ReverseTable reverseTable) {
        out = of(reverseTable.parent());
    }

    @Override
    public void visit(SortTable sortTable) {
        out = of(sortTable.parent());
    }

    @Override
    public void visit(SnapshotTable snapshotTable) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void visit(WhereTable whereTable) {
        out = of(whereTable.parent());
    }

    @Override
    public void visit(WhereInTable whereInTable) {
        out = of(whereInTable.left());
    }

    @Override
    public void visit(WhereNotInTable whereNotInTable) {
        out = of(whereNotInTable.left());
    }

    @Override
    public void visit(NaturalJoinTable naturalJoinTable) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void visit(ExactJoinTable exactJoinTable) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void visit(JoinTable joinTable) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void visit(LeftJoinTable leftJoinTable) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void visit(AsOfJoinTable aj) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void visit(ReverseAsOfJoinTable raj) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void visit(ViewTable viewTable) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void visit(UpdateViewTable updateViewTable) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void visit(UpdateTable updateTable) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void visit(SelectTable selectTable) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void visit(ByTable byTable) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void visit(AggregationTable aggregationTable) {
        throw new UnsupportedOperationException("todo");
    }
}
