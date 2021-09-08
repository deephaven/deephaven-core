package io.deephaven.qst;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.TableAdapterResults.Output;
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
import io.deephaven.qst.table.ParentsVisitor;
import io.deephaven.qst.table.ReverseAsOfJoinTable;
import io.deephaven.qst.table.ReverseTable;
import io.deephaven.qst.table.SelectTable;
import io.deephaven.qst.table.SingleParentTable;
import io.deephaven.qst.table.SnapshotTable;
import io.deephaven.qst.table.SortTable;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TableSpec.Visitor;
import io.deephaven.qst.table.TailTable;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.table.TimeTable;
import io.deephaven.qst.table.UpdateTable;
import io.deephaven.qst.table.UpdateViewTable;
import io.deephaven.qst.table.ViewTable;
import io.deephaven.qst.table.WhereInTable;
import io.deephaven.qst.table.WhereNotInTable;
import io.deephaven.qst.table.WhereTable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

class TableAdapterImpl<TOPS extends TableOperations<TOPS, TABLE>, TABLE> implements Visitor {

    static <TOPS extends TableOperations<TOPS, TABLE>, TABLE> TableAdapterResults<TOPS, TABLE> of(
            TableCreator<TABLE> creation, TableCreator.TableToOperations<TOPS, TABLE> toOps,
            TableCreator.OperationsToTable<TOPS, TABLE> toTable, Iterable<TableSpec> tables) {
        final TableAdapterImpl<TOPS, TABLE> visitor =
                new TableAdapterImpl<>(creation, toOps, toTable);
        ParentsVisitor.postOrderWalk(tables, visitor);
        return visitor.getOut();
    }

    static <TOPS extends TableOperations<TOPS, TABLE>, TABLE> TableAdapterResults<TOPS, TABLE> of(
            TableCreator<TABLE> creation, TableCreator.TableToOperations<TOPS, TABLE> toOps,
            TableCreator.OperationsToTable<TOPS, TABLE> toTable, TableSpec table) {
        return of(creation, toOps, toTable, Collections.singleton(table));
    }

    private final TableCreator<TABLE> tableCreation;
    private final TableCreator.TableToOperations<TOPS, TABLE> toOps;
    private final TableCreator.OperationsToTable<TOPS, TABLE> toTable;
    private final Map<TableSpec, Output<TOPS, TABLE>> outputs;

    private TableAdapterImpl(TableCreator<TABLE> tableCreation,
            TableCreator.TableToOperations<TOPS, TABLE> toOps,
            TableCreator.OperationsToTable<TOPS, TABLE> toTable) {
        this.tableCreation = Objects.requireNonNull(tableCreation);
        this.toOps = Objects.requireNonNull(toOps);
        this.toTable = Objects.requireNonNull(toTable);
        this.outputs = new LinkedHashMap<>();
    }

    public TableAdapterResults<TOPS, TABLE> getOut() {
        return ImmutableTableAdapterResults.<TOPS, TABLE>builder().putAllMap(outputs).build();
    }

    private TOPS parentOps(SingleParentTable table) {
        return ops(table.parent());
    }

    private TOPS ops(TableSpec table) {
        return outputs.get(table).walk(new GetOp()).getOut();
    }

    private TABLE table(TableSpec table) {
        return outputs.get(table).walk(new GetTable()).getOut();
    }

    private void addTable(TableSpec table, TABLE t) {
        if (outputs.putIfAbsent(table, new OutputTable(t)) != null) {
            throw new IllegalStateException();
        }
    }

    private void addOp(TableSpec table, TOPS t) {
        if (outputs.putIfAbsent(table, new OutputOp(t)) != null) {
            throw new IllegalStateException();
        }
    }

    @Override
    public void visit(EmptyTable emptyTable) {
        addTable(emptyTable, tableCreation.of(emptyTable));
    }

    @Override
    public void visit(NewTable newTable) {
        addTable(newTable, tableCreation.of(newTable));
    }

    @Override
    public void visit(TimeTable timeTable) {
        addTable(timeTable, tableCreation.of(timeTable));
    }

    @Override
    public void visit(MergeTable mergeTable) {
        List<TABLE> tables =
                mergeTable.tables().stream().map(this::table).collect(Collectors.toList());
        addTable(mergeTable, tableCreation.merge(tables));
    }

    @Override
    public void visit(HeadTable headTable) {
        addOp(headTable, parentOps(headTable).head(headTable.size()));
    }

    @Override
    public void visit(TailTable tailTable) {
        addOp(tailTable, parentOps(tailTable).tail(tailTable.size()));
    }

    @Override
    public void visit(ReverseTable reverseTable) {
        addOp(reverseTable, parentOps(reverseTable).reverse());
    }

    @Override
    public void visit(SortTable sortTable) {
        addOp(sortTable, parentOps(sortTable).sort(sortTable.columns()));
    }

    @Override
    public void visit(SnapshotTable snapshotTable) {
        final TOPS trigger = ops(snapshotTable.trigger());
        final TABLE base = table(snapshotTable.base());
        addOp(snapshotTable, trigger.snapshot(base, snapshotTable.doInitialSnapshot(),
                snapshotTable.stampColumns()));
    }

    @Override
    public void visit(WhereTable whereTable) {
        addOp(whereTable, parentOps(whereTable).where(whereTable.filters()));
    }

    @Override
    public void visit(WhereInTable whereInTable) {
        final TOPS left = ops(whereInTable.left());
        final TABLE right = table(whereInTable.right());
        addOp(whereInTable, left.whereIn(right, whereInTable.matches()));
    }

    @Override
    public void visit(WhereNotInTable whereNotInTable) {
        final TOPS left = ops(whereNotInTable.left());
        final TABLE right = table(whereNotInTable.right());
        addOp(whereNotInTable, left.whereNotIn(right, whereNotInTable.matches()));
    }

    @Override
    public void visit(ViewTable viewTable) {
        addOp(viewTable, parentOps(viewTable).view(viewTable.columns()));
    }

    @Override
    public void visit(SelectTable selectTable) {
        addOp(selectTable, parentOps(selectTable).select(selectTable.columns()));
    }

    @Override
    public void visit(UpdateViewTable updateViewTable) {
        addOp(updateViewTable, parentOps(updateViewTable).updateView(updateViewTable.columns()));
    }

    @Override
    public void visit(UpdateTable updateTable) {
        addOp(updateTable, parentOps(updateTable).update(updateTable.columns()));
    }

    @Override
    public void visit(NaturalJoinTable naturalJoinTable) {
        final TOPS left = ops(naturalJoinTable.left());
        final TABLE right = table(naturalJoinTable.right());
        addOp(naturalJoinTable,
                left.naturalJoin(right, naturalJoinTable.matches(), naturalJoinTable.additions()));
    }

    @Override
    public void visit(ExactJoinTable exactJoinTable) {
        final TOPS left = ops(exactJoinTable.left());
        final TABLE right = table(exactJoinTable.right());
        addOp(exactJoinTable,
                left.exactJoin(right, exactJoinTable.matches(), exactJoinTable.additions()));
    }

    @Override
    public void visit(JoinTable joinTable) {
        final TOPS left = ops(joinTable.left());
        final TABLE right = table(joinTable.right());
        addOp(joinTable,
                left.join(right, joinTable.matches(), joinTable.additions(), joinTable.reserveBits()));
    }

    @Override
    public void visit(LeftJoinTable leftJoinTable) {
        final TOPS left = ops(leftJoinTable.left());
        final TABLE right = table(leftJoinTable.right());
        addOp(leftJoinTable,
                left.exactJoin(right, leftJoinTable.matches(), leftJoinTable.additions()));
    }

    @Override
    public void visit(AsOfJoinTable aj) {
        final TOPS left = ops(aj.left());
        final TABLE right = table(aj.right());
        addOp(aj, left.aj(right, aj.matches(), aj.additions(), aj.rule()));
    }

    @Override
    public void visit(ReverseAsOfJoinTable raj) {
        final TOPS left = ops(raj.left());
        final TABLE right = table(raj.right());
        addOp(raj, left.exactJoin(right, raj.matches(), raj.additions()));
    }

    @Override
    public void visit(ByTable byTable) {
        addOp(byTable, parentOps(byTable).by(byTable.columns()));
    }

    @Override
    public void visit(AggregationTable aggregationTable) {
        addOp(aggregationTable, parentOps(aggregationTable).by(aggregationTable.columns(),
                aggregationTable.aggregations()));
    }

    @Override
    public void visit(TicketTable ticketTable) {
        addTable(ticketTable, tableCreation.of(ticketTable));
    }

    private final class OutputTable implements Output<TOPS, TABLE> {
        private final TABLE table;

        OutputTable(TABLE table) {
            this.table = Objects.requireNonNull(table);
        }

        TOPS toOps() {
            return toOps.of(table);
        }

        @Override
        public <V extends Visitor<TOPS, TABLE>> V walk(V visitor) {
            visitor.visit(table);
            return visitor;
        }
    }

    private final class OutputOp implements Output<TOPS, TABLE> {
        private final TOPS op;

        OutputOp(TOPS op) {
            this.op = Objects.requireNonNull(op);
        }

        TABLE toTable() {
            return toTable.of(op);
        }

        @Override
        public <V extends Visitor<TOPS, TABLE>> V walk(V visitor) {
            visitor.visit(op);
            return visitor;
        }
    }

    private final class GetTable implements Output.Visitor<TOPS, TABLE> {

        private TABLE out;

        public TABLE getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(TOPS tops) {
            out = toTable.of(tops);
        }

        @Override
        public void visit(TABLE table) {
            out = table;
        }
    }

    private final class GetOp implements Output.Visitor<TOPS, TABLE> {

        private TOPS out;

        public TOPS getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(TOPS tops) {
            out = tops;
        }

        @Override
        public void visit(TABLE table) {
            out = toOps.of(table);
        }
    }
}
