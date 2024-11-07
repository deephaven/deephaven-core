//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

public abstract class TableVisitorGeneric<T> implements TableSpec.Visitor<T> {

    public abstract T accept(TableSpec t);

    @Override
    public T visit(EmptyTable emptyTable) {
        return accept(emptyTable);
    }

    @Override
    public T visit(NewTable newTable) {
        return accept(newTable);
    }

    @Override
    public T visit(TimeTable timeTable) {
        return accept(timeTable);
    }

    @Override
    public T visit(MergeTable mergeTable) {
        return accept(mergeTable);
    }

    @Override
    public T visit(HeadTable headTable) {
        return accept(headTable);
    }

    @Override
    public T visit(TailTable tailTable) {
        return accept(tailTable);
    }

    @Override
    public T visit(SliceTable sliceTable) {
        return accept(sliceTable);
    }

    @Override
    public T visit(ReverseTable reverseTable) {
        return accept(reverseTable);
    }

    @Override
    public T visit(SortTable sortTable) {
        return accept(sortTable);
    }

    @Override
    public T visit(SnapshotTable snapshotTable) {
        return accept(snapshotTable);
    }

    @Override
    public T visit(SnapshotWhenTable snapshotWhenTable) {
        return accept(snapshotWhenTable);
    }

    @Override
    public T visit(WhereTable whereTable) {
        return accept(whereTable);
    }

    @Override
    public T visit(WhereInTable whereInTable) {
        return accept(whereInTable);
    }

    @Override
    public T visit(NaturalJoinTable naturalJoinTable) {
        return accept(naturalJoinTable);
    }

    @Override
    public T visit(ExactJoinTable exactJoinTable) {
        return accept(exactJoinTable);
    }

    @Override
    public T visit(JoinTable joinTable) {
        return accept(joinTable);
    }

    @Override
    public T visit(AsOfJoinTable aj) {
        return accept(aj);
    }

    @Override
    public T visit(RangeJoinTable rangeJoinTable) {
        return accept(rangeJoinTable);
    }

    @Override
    public T visit(ViewTable viewTable) {
        return accept(viewTable);
    }

    @Override
    public T visit(SelectTable selectTable) {
        return accept(selectTable);
    }

    @Override
    public T visit(UpdateViewTable updateViewTable) {
        return accept(updateViewTable);
    }

    @Override
    public T visit(UpdateTable updateTable) {
        return accept(updateTable);
    }

    @Override
    public T visit(LazyUpdateTable lazyUpdateTable) {
        return accept(lazyUpdateTable);
    }

    @Override
    public T visit(AggregateAllTable aggregateAllTable) {
        return accept(aggregateAllTable);
    }

    @Override
    public T visit(AggregateTable aggregateTable) {
        return accept(aggregateTable);
    }

    @Override
    public T visit(TicketTable ticketTable) {
        return accept(ticketTable);
    }

    @Override
    public T visit(InputTable inputTable) {
        return accept(inputTable);
    }

    @Override
    public T visit(SelectDistinctTable selectDistinctTable) {
        return accept(selectDistinctTable);
    }

    @Override
    public T visit(UpdateByTable updateByTable) {
        return accept(updateByTable);
    }

    @Override
    public T visit(UngroupTable ungroupTable) {
        return accept(ungroupTable);
    }

    @Override
    public T visit(DropColumnsTable dropColumnsTable) {
        return accept(dropColumnsTable);
    }

    @Override
    public T visit(MultiJoinTable multiJoinTable) {
        return accept(multiJoinTable);
    }
}
