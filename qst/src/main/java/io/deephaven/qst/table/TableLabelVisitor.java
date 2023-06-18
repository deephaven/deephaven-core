/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.api.SortColumn;
import io.deephaven.api.Strings;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

public class TableLabelVisitor extends TableVisitorGeneric {

    /**
     * Constructs a non-recursive label for {@code table}.
     *
     * <p>
     * Callers can use this for visualization and debugging purposes, but should not depend on the strings being equal
     * from release to release.
     *
     * @param table the table spec
     * @return the label
     */
    public static String of(TableSpec table) {
        return table.walk(new TableLabelVisitor(new StringBuilder())).sb.toString();
    }

    final StringBuilder sb;

    public TableLabelVisitor(StringBuilder sb) {
        this.sb = Objects.requireNonNull(sb);
    }

    @Override
    public void accept(TableSpec t) {
        // All tables are ImmutableX - we want the non-immutable version of the class, slightly
        // prettier.
        sb.append(t.getClass().getSuperclass().getSimpleName());
    }

    @Override
    public void visit(EmptyTable emptyTable) {
        sb.append("empty(").append(emptyTable.size()).append(')');
    }

    @Override
    public void visit(TimeTable timeTable) {
        sb.append("time(").append(timeTable.interval()).append(')');
    }

    @Override
    public void visit(MergeTable mergeTable) {
        sb.append("merge()");
    }

    @Override
    public void visit(NewTable newTable) {
        sb.append("newTable(").append(newTable.size()).append(", ").append(newTable.header()).append(')');
    }

    @Override
    public void visit(HeadTable headTable) {
        sb.append("head(").append(headTable.size()).append(')');
    }

    @Override
    public void visit(TailTable tailTable) {
        sb.append("tail(").append(tailTable.size()).append(')');
    }

    @Override
    public void visit(NaturalJoinTable naturalJoinTable) {
        join("naturalJoin", naturalJoinTable);
    }

    @Override
    public void visit(ExactJoinTable exactJoinTable) {
        join("exactJoin", exactJoinTable);
    }

    @Override
    public void visit(JoinTable joinTable) {
        join("join", joinTable);
    }

    @Override
    public void visit(AsOfJoinTable aj) {
        sb.append("asOfJoin([");
        append(Strings::of, aj.matches(), sb);
        sb.append("],");
        sb.append(Strings.of(aj.joinMatch()));
        sb.append(",");
        append(Strings::of, aj.additions(), sb);
        sb.append("])");
    }

    @Override
    public void visit(RangeJoinTable rangeJoinTable) {
        sb.append("rangeJoin([");
        append(Strings::of, rangeJoinTable.exactMatches(), sb);
        sb.append("],");
        sb.append(Strings.of(rangeJoinTable.rangeMatch()));
        sb.append(",");
        sb.append(Strings.ofAggregations(rangeJoinTable.aggregations()));
        sb.append(")");
    }

    @Override
    public void visit(ViewTable viewTable) {
        selectable("view", viewTable);
    }

    @Override
    public void visit(SelectTable selectTable) {
        selectable("select", selectTable);
    }

    @Override
    public void visit(UpdateViewTable updateViewTable) {
        selectable("updateView", updateViewTable);
    }

    @Override
    public void visit(UpdateTable updateTable) {
        selectable("update", updateTable);
    }

    @Override
    public void visit(LazyUpdateTable lazyUpdateTable) {
        selectable("lazyUpdate", lazyUpdateTable);
    }

    @Override
    public void visit(WhereTable whereTable) {
        sb.append("where(");
        sb.append(Strings.of(whereTable.filter()));
        sb.append(')');
    }

    @Override
    public void visit(AggregateAllTable aggregateAllTable) {
        sb.append("aggAllBy(");
        sb.append(aggregateAllTable.spec().description()).append(',');
        append(Strings::of, aggregateAllTable.groupByColumns(), sb);
        sb.append(')');
    }

    @Override
    public void visit(AggregateTable aggregateTable) {
        sb.append("aggBy([");
        append(Strings::of, aggregateTable.groupByColumns(), sb);
        sb.append("],");
        sb.append(Strings.ofAggregations(aggregateTable.aggregations()));
        sb.append(")");
    }

    @Override
    public void visit(TicketTable ticketTable) {
        sb.append(String.format("ticketTable(%s)", new String(ticketTable.ticket(), StandardCharsets.UTF_8)));
    }

    @Override
    public void visit(InputTable inputTable) {
        inputTable.walk(new InputTable.Visitor() {
            @Override
            public void visit(InMemoryAppendOnlyInputTable inMemoryAppendOnly) {
                sb.append("InMemoryAppendOnlyInputTable(...)");
            }

            @Override
            public void visit(InMemoryKeyBackedInputTable inMemoryKeyBacked) {
                sb.append("InMemoryKeyBackedInputTable(...)");
            }
        });
    }

    @Override
    public void visit(SelectDistinctTable selectDistinctTable) {
        sb.append("selectDistinct(");
        append(Strings::of, selectDistinctTable.columns(), sb);
        sb.append(')');
    }

    @Override
    public void visit(UpdateByTable updateByTable) {
        // TODO(deephaven-core#1116): Add labeling, or structuring, for qst graphviz aggregations
        sb.append("updateBy([");
        append(Strings::of, updateByTable.groupByColumns(), sb);
        sb.append("],[ todo ])");
    }

    @Override
    public void visit(UngroupTable ungroupTable) {
        sb.append("ungroup(").append(ungroupTable.nullFill()).append(",[");
        append(Strings::of, ungroupTable.ungroupColumns(), sb);
        sb.append("])");
    }

    @Override
    public void visit(DropColumnsTable dropColumnsTable) {
        sb.append("dropColumns([");
        append(Strings::of, dropColumnsTable.dropColumns(), sb);
        sb.append("])");
    }

    @Override
    public void visit(ReverseTable reverseTable) {
        sb.append("reverse()");
    }

    @Override
    public void visit(SortTable sortTable) {
        sb.append("sort([");
        append(TableLabelVisitor::toString, sortTable.columns(), sb);
        sb.append("])");
    }

    @Override
    public void visit(SnapshotTable snapshotTable) {
        sb.append("snapshot()");
    }

    private void join(String name, Join j) {
        sb.append(name).append("([");
        append(Strings::of, j.matches(), sb);
        sb.append("],[");
        append(Strings::of, j.additions(), sb);
        sb.append("])");
    }

    private void selectable(String name, SelectableTable j) {
        sb.append(name).append('(');
        append(Strings::of, j.columns(), sb);
        sb.append(')');
    }

    private static <T> void append(Function<T, String> f, Collection<T> c, StringBuilder sb) {
        Iterator<T> it = c.iterator();
        if (it.hasNext()) {
            sb.append(f.apply(it.next()));
        }
        while (it.hasNext()) {
            sb.append(',').append(f.apply(it.next()));
        }
    }

    private static String toString(SortColumn sort) {
        return String.format("%s(%s)", sort.order(), sort.column().name());
    }
}
