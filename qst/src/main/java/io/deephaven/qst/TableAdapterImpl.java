package io.deephaven.qst;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.table.AggregationTable;
import io.deephaven.qst.table.AsOfJoinTable;
import io.deephaven.qst.table.ByTable;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.ExactJoinTable;
import io.deephaven.qst.table.HeadTable;
import io.deephaven.qst.table.JoinTable;
import io.deephaven.qst.table.LeftJoinTable;
import io.deephaven.qst.table.MergeTable;
import io.deephaven.qst.table.NaturalJoinTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.ReverseAsOfJoinTable;
import io.deephaven.qst.table.ReverseTable;
import io.deephaven.qst.table.SelectTable;
import io.deephaven.qst.table.SingleParentTable;
import io.deephaven.qst.table.SnapshotTable;
import io.deephaven.qst.table.SortTable;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TableSpec.Visitor;
import io.deephaven.qst.table.TailTable;
import io.deephaven.qst.table.TimeTable;
import io.deephaven.qst.table.UpdateTable;
import io.deephaven.qst.table.UpdateViewTable;
import io.deephaven.qst.table.ViewTable;
import io.deephaven.qst.table.WhereInTable;
import io.deephaven.qst.table.WhereNotInTable;
import io.deephaven.qst.table.WhereTable;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class TableAdapterImpl<TOPS extends TableOperations<TOPS, TABLE>, TABLE> implements Visitor {

    static <TOPS extends TableOperations<TOPS, TABLE>, TABLE> TABLE toTable(
        TableCreator<TABLE> creation, TableCreator.TableToOperations<TOPS, TABLE> toOps,
        TableCreator.OperationsToTable<TOPS, TABLE> toTable, TableSpec table) {
        return table.walk(new TableAdapterImpl<>(creation, toOps, toTable)).getTableOut();
    }

    static <TOPS extends TableOperations<TOPS, TABLE>, TABLE> TOPS toOperations(
        TableCreator<TABLE> creation, TableCreator.TableToOperations<TOPS, TABLE> toOps,
        TableCreator.OperationsToTable<TOPS, TABLE> toTable, TableSpec table) {
        return table.walk(new TableAdapterImpl<>(creation, toOps, toTable)).getOperationsOut();
    }

    private final TableCreator<TABLE> tableCreation;
    private final TableCreator.TableToOperations<TOPS, TABLE> toOps;
    private final TableCreator.OperationsToTable<TOPS, TABLE> toTable;

    private TABLE tableOut;
    private TOPS topsOut;

    private TableAdapterImpl(TableCreator<TABLE> tableCreation,
        TableCreator.TableToOperations<TOPS, TABLE> toOps,
        TableCreator.OperationsToTable<TOPS, TABLE> toTable) {
        this.tableCreation = Objects.requireNonNull(tableCreation);
        this.toOps = Objects.requireNonNull(toOps);
        this.toTable = Objects.requireNonNull(toTable);
    }

    public TABLE getTableOut() {
        if (tableOut != null) {
            return tableOut;
        }
        if (topsOut != null) {
            return toTable.of(topsOut);
        }
        throw new IllegalStateException();
    }

    public TOPS getOperationsOut() {
        if (tableOut != null) {
            return toOps.of(tableOut);
        }
        if (topsOut != null) {
            return topsOut;
        }
        throw new IllegalStateException();
    }

    private TOPS parentOps(SingleParentTable table) {
        return ops(table.parent());
    }

    private TOPS ops(TableSpec table) {
        return toOperations(tableCreation, toOps, toTable, table);
    }

    private TABLE table(TableSpec table) {
        return toTable(tableCreation, toOps, toTable, table);
    }

    @Override
    public void visit(EmptyTable emptyTable) {
        tableOut = tableCreation.of(emptyTable);
    }

    @Override
    public void visit(NewTable newTable) {
        tableOut = tableCreation.of(newTable);
    }

    @Override
    public void visit(TimeTable timeTable) {
        tableOut = tableCreation.of(timeTable);
    }

    @Override
    public void visit(MergeTable mergeTable) {
        List<TABLE> tables =
            mergeTable.tables().stream().map(this::table).collect(Collectors.toList());
        tableOut = tableCreation.merge(tables);
    }

    @Override
    public void visit(HeadTable headTable) {
        topsOut = parentOps(headTable).head(headTable.size());
    }

    @Override
    public void visit(TailTable tailTable) {
        topsOut = parentOps(tailTable).tail(tailTable.size());
    }

    @Override
    public void visit(ReverseTable reverseTable) {
        topsOut = parentOps(reverseTable).reverse();
    }

    @Override
    public void visit(SortTable sortTable) {
        topsOut = parentOps(sortTable).sort(sortTable.columns());
    }

    @Override
    public void visit(SnapshotTable snapshotTable) {
        final TOPS trigger = ops(snapshotTable.trigger());
        final TABLE base = table(snapshotTable.base());
        topsOut =
            trigger.snapshot(base, snapshotTable.doInitialSnapshot(), snapshotTable.stampColumns());
    }

    @Override
    public void visit(WhereTable whereTable) {
        topsOut = parentOps(whereTable).where(whereTable.filters());
    }

    @Override
    public void visit(WhereInTable whereInTable) {
        final TOPS left = ops(whereInTable.left());
        final TABLE right = table(whereInTable.right());
        topsOut = left.whereIn(right, whereInTable.matches());
    }

    @Override
    public void visit(WhereNotInTable whereNotInTable) {
        final TOPS left = ops(whereNotInTable.left());
        final TABLE right = table(whereNotInTable.right());
        topsOut = left.whereNotIn(right, whereNotInTable.matches());
    }

    @Override
    public void visit(ViewTable viewTable) {
        topsOut = parentOps(viewTable).view(viewTable.columns());
    }

    @Override
    public void visit(SelectTable selectTable) {
        topsOut = parentOps(selectTable).select(selectTable.columns());
    }

    @Override
    public void visit(UpdateViewTable updateViewTable) {
        topsOut = parentOps(updateViewTable).updateView(updateViewTable.columns());
    }

    @Override
    public void visit(UpdateTable updateTable) {
        topsOut = parentOps(updateTable).update(updateTable.columns());
    }

    @Override
    public void visit(NaturalJoinTable naturalJoinTable) {
        final TOPS left = ops(naturalJoinTable.left());
        final TABLE right = table(naturalJoinTable.right());
        topsOut = left.naturalJoin(right, naturalJoinTable.matches(), naturalJoinTable.additions());
    }

    @Override
    public void visit(ExactJoinTable exactJoinTable) {
        final TOPS left = ops(exactJoinTable.left());
        final TABLE right = table(exactJoinTable.right());
        topsOut = left.exactJoin(right, exactJoinTable.matches(), exactJoinTable.additions());
    }

    @Override
    public void visit(JoinTable joinTable) {
        final TOPS left = ops(joinTable.left());
        final TABLE right = table(joinTable.right());
        topsOut =
            left.join(right, joinTable.matches(), joinTable.additions(), joinTable.reserveBits());
    }

    @Override
    public void visit(LeftJoinTable leftJoinTable) {
        final TOPS left = ops(leftJoinTable.left());
        final TABLE right = table(leftJoinTable.right());
        topsOut = left.exactJoin(right, leftJoinTable.matches(), leftJoinTable.additions());
    }

    @Override
    public void visit(AsOfJoinTable aj) {
        final TOPS left = ops(aj.left());
        final TABLE right = table(aj.right());
        topsOut = left.aj(right, aj.matches(), aj.additions(), aj.rule());
    }

    @Override
    public void visit(ReverseAsOfJoinTable raj) {
        final TOPS left = ops(raj.left());
        final TABLE right = table(raj.right());
        topsOut = left.exactJoin(right, raj.matches(), raj.additions());
    }

    @Override
    public void visit(ByTable byTable) {
        topsOut = parentOps(byTable).by(byTable.columns());
    }

    @Override
    public void visit(AggregationTable aggregationTable) {
        topsOut = parentOps(aggregationTable).by(aggregationTable.columns(),
            aggregationTable.aggregations());
    }
}
