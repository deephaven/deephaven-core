//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst;

import io.deephaven.api.ColumnName;
import io.deephaven.api.TableOperations;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.qst.TableAdapterResults.Output;
import io.deephaven.qst.table.AggregateAllTable;
import io.deephaven.qst.table.AggregateTable;
import io.deephaven.qst.table.AsOfJoinTable;
import io.deephaven.qst.table.DropColumnsTable;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.ExactJoinTable;
import io.deephaven.qst.table.HeadTable;
import io.deephaven.qst.table.InputTable;
import io.deephaven.qst.table.JoinTable;
import io.deephaven.qst.table.LazyUpdateTable;
import io.deephaven.qst.table.MergeTable;
import io.deephaven.qst.table.MultiJoinInput;
import io.deephaven.qst.table.MultiJoinTable;
import io.deephaven.qst.table.NaturalJoinTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.ParentsVisitor;
import io.deephaven.qst.table.RangeJoinTable;
import io.deephaven.qst.table.ReverseTable;
import io.deephaven.qst.table.SelectDistinctTable;
import io.deephaven.qst.table.SelectTable;
import io.deephaven.qst.table.SingleParentTable;
import io.deephaven.qst.table.SliceTable;
import io.deephaven.qst.table.SnapshotTable;
import io.deephaven.qst.table.SnapshotWhenTable;
import io.deephaven.qst.table.SortTable;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TableSpec.Visitor;
import io.deephaven.qst.table.TailTable;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.table.TimeTable;
import io.deephaven.qst.table.UngroupTable;
import io.deephaven.qst.table.UpdateByTable;
import io.deephaven.qst.table.UpdateTable;
import io.deephaven.qst.table.UpdateViewTable;
import io.deephaven.qst.table.ViewTable;
import io.deephaven.qst.table.WhereInTable;
import io.deephaven.qst.table.WhereTable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

class TableAdapterImpl<TOPS extends TableOperations<TOPS, TABLE>, TABLE> implements Visitor<Void> {

    // Note: instead of having the visitor recursively resolve dependencies, we are explicitly walking all nodes of the
    // tree in post-order. In some sense, emulating a recursive ordering, but it explicitly solves some state management
    // complexity with a recursive implementation.

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
        return outputs.get(table).walk(new GetOp());
    }

    private TABLE table(TableSpec table) {
        return outputs.get(table).walk(new GetTable());
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
    public Void visit(EmptyTable emptyTable) {
        addTable(emptyTable, tableCreation.of(emptyTable));
        return null;
    }

    @Override
    public Void visit(NewTable newTable) {
        addTable(newTable, tableCreation.of(newTable));
        return null;
    }

    @Override
    public Void visit(TimeTable timeTable) {
        addTable(timeTable, tableCreation.of(timeTable));
        return null;
    }

    @Override
    public Void visit(MultiJoinTable multiJoinTable) {
        final List<MultiJoinInput<TABLE>> inputs =
                multiJoinTable.inputs().stream().map(this::adapt).collect(Collectors.toList());
        addTable(multiJoinTable, tableCreation.multiJoin(inputs));
        return null;
    }

    private MultiJoinInput<TABLE> adapt(MultiJoinInput<TableSpec> input) {
        return MultiJoinInput.<TABLE>builder()
                .table(table(input.table()))
                .addAllMatches(input.matches())
                .addAllAdditions(input.additions())
                .build();
    }

    @Override
    public Void visit(MergeTable mergeTable) {
        List<TABLE> tables =
                mergeTable.tables().stream().map(this::table).collect(Collectors.toList());
        addTable(mergeTable, tableCreation.merge(tables));
        return null;
    }

    @Override
    public Void visit(HeadTable headTable) {
        addOp(headTable, parentOps(headTable).head(headTable.size()));
        return null;
    }

    @Override
    public Void visit(TailTable tailTable) {
        addOp(tailTable, parentOps(tailTable).tail(tailTable.size()));
        return null;
    }

    @Override
    public Void visit(SliceTable sliceTable) {
        addOp(sliceTable,
                parentOps(sliceTable).slice(sliceTable.firstPositionInclusive(), sliceTable.lastPositionExclusive()));
        return null;
    }

    @Override
    public Void visit(ReverseTable reverseTable) {
        addOp(reverseTable, parentOps(reverseTable).reverse());
        return null;
    }

    @Override
    public Void visit(SortTable sortTable) {
        addOp(sortTable, parentOps(sortTable).sort(sortTable.columns()));
        return null;
    }

    @Override
    public Void visit(SnapshotTable snapshotTable) {
        final TOPS base = ops(snapshotTable.base());
        addOp(snapshotTable, base.snapshot());
        return null;
    }

    @Override
    public Void visit(SnapshotWhenTable snapshotWhenTable) {
        final TOPS base = ops(snapshotWhenTable.base());
        final TABLE trigger = table(snapshotWhenTable.trigger());
        addOp(snapshotWhenTable, base.snapshotWhen(trigger, snapshotWhenTable.options()));
        return null;
    }

    @Override
    public Void visit(WhereTable whereTable) {
        addOp(whereTable, parentOps(whereTable).where(whereTable.filter()));
        return null;
    }

    @Override
    public Void visit(WhereInTable whereInTable) {
        final TOPS left = ops(whereInTable.left());
        final TABLE right = table(whereInTable.right());
        final TOPS result = whereInTable.inverted() ? left.whereNotIn(right, whereInTable.matches())
                : left.whereIn(right, whereInTable.matches());
        addOp(whereInTable, result);
        return null;
    }

    @Override
    public Void visit(ViewTable viewTable) {
        addOp(viewTable, parentOps(viewTable).view(viewTable.columns()));
        return null;
    }

    @Override
    public Void visit(SelectTable selectTable) {
        addOp(selectTable, parentOps(selectTable).select(selectTable.columns()));
        return null;
    }

    @Override
    public Void visit(UpdateViewTable updateViewTable) {
        addOp(updateViewTable, parentOps(updateViewTable).updateView(updateViewTable.columns()));
        return null;
    }

    @Override
    public Void visit(UpdateTable updateTable) {
        addOp(updateTable, parentOps(updateTable).update(updateTable.columns()));
        return null;
    }

    @Override
    public Void visit(LazyUpdateTable lazyUpdateTable) {
        addOp(lazyUpdateTable, parentOps(lazyUpdateTable).lazyUpdate(lazyUpdateTable.columns()));
        return null;
    }

    @Override
    public Void visit(NaturalJoinTable naturalJoinTable) {
        final TOPS left = ops(naturalJoinTable.left());
        final TABLE right = table(naturalJoinTable.right());
        addOp(naturalJoinTable,
                left.naturalJoin(right, naturalJoinTable.matches(), naturalJoinTable.additions()));
        return null;
    }

    @Override
    public Void visit(ExactJoinTable exactJoinTable) {
        final TOPS left = ops(exactJoinTable.left());
        final TABLE right = table(exactJoinTable.right());
        addOp(exactJoinTable,
                left.exactJoin(right, exactJoinTable.matches(), exactJoinTable.additions()));
        return null;
    }

    @Override
    public Void visit(JoinTable joinTable) {
        final TOPS left = ops(joinTable.left());
        final TABLE right = table(joinTable.right());
        if (joinTable.reserveBits().isPresent()) {
            addOp(joinTable,
                    left.join(right, joinTable.matches(), joinTable.additions(), joinTable.reserveBits().getAsInt()));
        } else {
            addOp(joinTable,
                    left.join(right, joinTable.matches(), joinTable.additions()));
        }
        return null;
    }

    @Override
    public Void visit(AsOfJoinTable aj) {
        final TOPS left = ops(aj.left());
        final TABLE right = table(aj.right());
        addOp(aj, left.asOfJoin(right, aj.matches(), aj.joinMatch(), aj.additions()));
        return null;
    }

    @Override
    public Void visit(RangeJoinTable rangeJoinTable) {
        final TOPS left = ops(rangeJoinTable.left());
        final TABLE right = table(rangeJoinTable.right());
        addOp(rangeJoinTable, left.rangeJoin(
                right, rangeJoinTable.exactMatches(), rangeJoinTable.rangeMatch(), rangeJoinTable.aggregations()));
        return null;
    }

    @Override
    public Void visit(AggregateAllTable aggregateAllTable) {
        final AggSpec spec = aggregateAllTable.spec();
        if (aggregateAllTable.groupByColumns().isEmpty()) {
            addOp(aggregateAllTable, parentOps(aggregateAllTable).aggAllBy(spec));
        } else {
            final ColumnName[] groupByColumns = aggregateAllTable.groupByColumns().toArray(new ColumnName[0]);
            addOp(aggregateAllTable, parentOps(aggregateAllTable).aggAllBy(spec, groupByColumns));
        }
        return null;
    }

    @Override
    public Void visit(AggregateTable aggregateTable) {
        if (aggregateTable.groupByColumns().isEmpty()) {
            addOp(aggregateTable, ops(aggregateTable.parent()).aggBy(
                    aggregateTable.aggregations(),
                    aggregateTable.preserveEmpty()));
        } else {
            addOp(aggregateTable, ops(aggregateTable.parent()).aggBy(
                    aggregateTable.aggregations(),
                    aggregateTable.preserveEmpty(),
                    aggregateTable.initialGroups().map(this::table).orElse(null),
                    aggregateTable.groupByColumns()));
        }
        return null;
    }

    @Override
    public Void visit(TicketTable ticketTable) {
        addTable(ticketTable, tableCreation.of(ticketTable));
        return null;
    }

    @Override
    public Void visit(InputTable inputTable) {
        addTable(inputTable, tableCreation.of(inputTable));
        return null;
    }

    @Override
    public Void visit(SelectDistinctTable selectDistinctTable) {
        if (selectDistinctTable.columns().isEmpty()) {
            addOp(selectDistinctTable, parentOps(selectDistinctTable).selectDistinct());
        } else {
            addOp(selectDistinctTable,
                    parentOps(selectDistinctTable).selectDistinct(selectDistinctTable.columns()));
        }
        return null;
    }

    @Override
    public Void visit(UpdateByTable updateByTable) {
        if (updateByTable.control().isPresent()) {
            addOp(updateByTable, parentOps(updateByTable).updateBy(
                    updateByTable.control().get(),
                    updateByTable.operations(),
                    updateByTable.groupByColumns()));
        } else {
            addOp(updateByTable, parentOps(updateByTable).updateBy(
                    updateByTable.operations(),
                    updateByTable.groupByColumns()));
        }
        return null;
    }

    @Override
    public Void visit(UngroupTable ungroupTable) {
        addOp(ungroupTable, parentOps(ungroupTable)
                .ungroup(ungroupTable.nullFill(), ungroupTable.ungroupColumns()));
        return null;
    }

    @Override
    public Void visit(DropColumnsTable dropColumnsTable) {
        addOp(dropColumnsTable,
                parentOps(dropColumnsTable).dropColumns(dropColumnsTable.dropColumns().toArray(new ColumnName[0])));
        return null;
    }

    private final class OutputTable implements Output<TOPS, TABLE> {
        private final TABLE table;

        OutputTable(TABLE table) {
            this.table = Objects.requireNonNull(table);
        }

        @Override
        public <T, V extends Visitor<T, TOPS, TABLE>> T walk(V visitor) {
            return visitor.visit(table);
        }
    }

    private final class OutputOp implements Output<TOPS, TABLE> {
        private final TOPS op;

        OutputOp(TOPS op) {
            this.op = Objects.requireNonNull(op);
        }

        @Override
        public <T, V extends Visitor<T, TOPS, TABLE>> T walk(V visitor) {
            return visitor.visit(op);
        }
    }

    private final class GetTable implements Output.Visitor<TABLE, TOPS, TABLE> {

        @Override
        public TABLE visit(TOPS tops) {
            return toTable.of(tops);
        }

        @Override
        public TABLE visit(TABLE table) {
            return table;
        }
    }

    private final class GetOp implements Output.Visitor<TOPS, TOPS, TABLE> {

        @Override
        public TOPS visit(TOPS tops) {
            return tops;
        }

        @Override
        public TOPS visit(TABLE table) {
            return toOps.of(table);
        }
    }
}
