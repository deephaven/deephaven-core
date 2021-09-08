package io.deephaven.qst.table;

public abstract class TableVisitorGeneric implements TableSpec.Visitor {

    public abstract void accept(TableSpec t);

    @Override
    public void visit(EmptyTable emptyTable) {
        accept(emptyTable);
    }

    @Override
    public void visit(NewTable newTable) {
        accept(newTable);
    }

    @Override
    public void visit(TimeTable timeTable) {
        accept(timeTable);
    }

    @Override
    public void visit(MergeTable mergeTable) {
        accept(mergeTable);
    }

    @Override
    public void visit(HeadTable headTable) {
        accept(headTable);
    }

    @Override
    public void visit(TailTable tailTable) {
        accept(tailTable);
    }

    @Override
    public void visit(ReverseTable reverseTable) {
        accept(reverseTable);
    }

    @Override
    public void visit(SortTable sortTable) {
        accept(sortTable);
    }

    @Override
    public void visit(SnapshotTable snapshotTable) {
        accept(snapshotTable);
    }

    @Override
    public void visit(WhereTable whereTable) {
        accept(whereTable);
    }

    @Override
    public void visit(WhereInTable whereInTable) {
        accept(whereInTable);
    }

    @Override
    public void visit(WhereNotInTable whereNotInTable) {
        accept(whereNotInTable);
    }

    @Override
    public void visit(NaturalJoinTable naturalJoinTable) {
        accept(naturalJoinTable);
    }

    @Override
    public void visit(ExactJoinTable exactJoinTable) {
        accept(exactJoinTable);
    }

    @Override
    public void visit(JoinTable joinTable) {
        accept(joinTable);
    }

    @Override
    public void visit(LeftJoinTable leftJoinTable) {
        accept(leftJoinTable);
    }

    @Override
    public void visit(AsOfJoinTable aj) {
        accept(aj);
    }

    @Override
    public void visit(ReverseAsOfJoinTable raj) {
        accept(raj);
    }

    @Override
    public void visit(ViewTable viewTable) {
        accept(viewTable);
    }

    @Override
    public void visit(SelectTable selectTable) {
        accept(selectTable);
    }

    @Override
    public void visit(UpdateViewTable updateViewTable) {
        accept(updateViewTable);
    }

    @Override
    public void visit(UpdateTable updateTable) {
        accept(updateTable);
    }

    @Override
    public void visit(ByTable byTable) {
        accept(byTable);
    }

    @Override
    public void visit(AggregationTable aggregationTable) {
        accept(aggregationTable);
    }

    @Override
    public void visit(TicketTable ticketTable) {
        accept(ticketTable);
    }
}
