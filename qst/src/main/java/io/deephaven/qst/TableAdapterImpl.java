/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst;

import io.deephaven.api.ColumnName;
import io.deephaven.api.TableOperations;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.qst.TableAdapterResults.Output;
import io.deephaven.qst.table.*;
import io.deephaven.qst.table.TableSpec.Visitor;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

class TableAdapterImpl<TOPS extends TableOperations<TOPS, TABLE>, TABLE> implements Visitor {

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
        final TOPS base = ops(snapshotTable.base());
        addOp(snapshotTable, base.snapshot());
    }

    @Override
    public void visit(SnapshotWhenTable snapshotWhenTable) {
        final TOPS base = ops(snapshotWhenTable.base());
        final TABLE trigger = table(snapshotWhenTable.trigger());
        addOp(snapshotWhenTable, base.snapshotWhen(trigger, snapshotWhenTable.options()));
    }

    @Override
    public void visit(WhereTable whereTable) {
        addOp(whereTable, parentOps(whereTable).where(whereTable.filter()));
    }

    @Override
    public void visit(WhereInTable whereInTable) {
        final TOPS left = ops(whereInTable.left());
        final TABLE right = table(whereInTable.right());
        final TOPS result = whereInTable.inverted() ? left.whereNotIn(right, whereInTable.matches())
                : left.whereIn(right, whereInTable.matches());
        addOp(whereInTable, result);
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
    public void visit(LazyUpdateTable lazyUpdateTable) {
        addOp(lazyUpdateTable, parentOps(lazyUpdateTable).lazyUpdate(lazyUpdateTable.columns()));
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
        if (joinTable.reserveBits().isPresent()) {
            addOp(joinTable,
                    left.join(right, joinTable.matches(), joinTable.additions(), joinTable.reserveBits().getAsInt()));
        } else {
            addOp(joinTable,
                    left.join(right, joinTable.matches(), joinTable.additions()));
        }
    }

    @Override
    public void visit(AsOfJoinTable aj) {
        final TOPS left = ops(aj.left());
        final TABLE right = table(aj.right());
        addOp(aj, left.asOfJoin(right, aj.matches(), aj.joinMatch(), aj.additions()));
    }

    @Override
    public void visit(RangeJoinTable rangeJoinTable) {
        final TOPS left = ops(rangeJoinTable.left());
        final TABLE right = table(rangeJoinTable.right());
        addOp(rangeJoinTable, left.rangeJoin(
                right, rangeJoinTable.exactMatches(), rangeJoinTable.rangeMatch(), rangeJoinTable.aggregations()));
    }

    @Override
    public void visit(AggregateAllTable aggregateAllTable) {
        final AggSpec spec = aggregateAllTable.spec();
        if (aggregateAllTable.groupByColumns().isEmpty()) {
            addOp(aggregateAllTable, parentOps(aggregateAllTable).aggAllBy(spec));
        } else {
            final ColumnName[] groupByColumns = aggregateAllTable.groupByColumns().toArray(new ColumnName[0]);
            addOp(aggregateAllTable, parentOps(aggregateAllTable).aggAllBy(spec, groupByColumns));
        }
    }

    @Override
    public void visit(AggregateTable aggregateTable) {
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
    }

    @Override
    public void visit(TicketTable ticketTable) {
        addTable(ticketTable, tableCreation.of(ticketTable));
    }

    @Override
    public void visit(InputTable inputTable) {
        addTable(inputTable, tableCreation.of(inputTable));
    }

    @Override
    public void visit(SelectDistinctTable selectDistinctTable) {
        if (selectDistinctTable.columns().isEmpty()) {
            addOp(selectDistinctTable, parentOps(selectDistinctTable).selectDistinct());
        } else {
            addOp(selectDistinctTable,
                    parentOps(selectDistinctTable).selectDistinct(selectDistinctTable.columns()));
        }
    }

    @Override
    public void visit(UpdateByTable updateByTable) {
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
    }

    @Override
    public void visit(UngroupTable ungroupTable) {
        addOp(ungroupTable, parentOps(ungroupTable)
                .ungroup(ungroupTable.nullFill(), ungroupTable.ungroupColumns()));
    }

    @Override
    public void visit(DropColumnsTable dropColumnsTable) {
        addOp(dropColumnsTable,
                parentOps(dropColumnsTable).dropColumns(dropColumnsTable.dropColumns().toArray(new ColumnName[0])));
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
