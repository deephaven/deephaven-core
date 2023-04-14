/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.graphviz;

import io.deephaven.api.Strings;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.AggregationDescriptions;
import io.deephaven.qst.table.AggregateAllTable;
import io.deephaven.qst.table.AggregateTable;
import io.deephaven.qst.table.AsOfJoinTable;
import io.deephaven.qst.table.DropColumnsTable;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.ExactJoinTable;
import io.deephaven.qst.table.HeadTable;
import io.deephaven.qst.table.InMemoryAppendOnlyInputTable;
import io.deephaven.qst.table.InMemoryKeyBackedInputTable;
import io.deephaven.qst.table.InputTable;
import io.deephaven.qst.table.Join;
import io.deephaven.qst.table.JoinTable;
import io.deephaven.qst.table.LazyUpdateTable;
import io.deephaven.qst.table.NaturalJoinTable;
import io.deephaven.qst.table.RangeJoinTable;
import io.deephaven.qst.table.ReverseAsOfJoinTable;
import io.deephaven.qst.table.SelectDistinctTable;
import io.deephaven.qst.table.SelectTable;
import io.deephaven.qst.table.SelectableTable;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TableVisitorGeneric;
import io.deephaven.qst.table.TailTable;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.table.TimeTable;
import io.deephaven.qst.table.UngroupTable;
import io.deephaven.qst.table.UpdateByTable;
import io.deephaven.qst.table.UpdateTable;
import io.deephaven.qst.table.UpdateViewTable;
import io.deephaven.qst.table.ViewTable;
import io.deephaven.qst.table.WhereTable;

import java.util.*;
import java.util.function.Function;

public class LabelBuilder extends TableVisitorGeneric {

    /**
     * Constructs a string label for the given {@code table}.
     *
     * @param table the table
     * @return the label
     */
    public static String of(TableSpec table) {
        return table.walk(new LabelBuilder(new StringBuilder())).sb.toString();
    }

    private final StringBuilder sb;

    public LabelBuilder(StringBuilder sb) {
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
        join("aj", aj);
    }

    @Override
    public void visit(ReverseAsOfJoinTable raj) {
        join("raj", raj);
    }

    @Override
    public void visit(RangeJoinTable rangeJoinTable) {
        sb.append("rangeJoin([");
        append(Strings::of, rangeJoinTable.exactMatches(), sb);
        sb.append("],");
        sb.append(Strings.of(rangeJoinTable.rangeMatch()));
        sb.append(",[");
        append(rangeJoinTable.aggregations(), sb);
        sb.append("])");
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
        append(Strings::of, whereTable.filters(), sb);
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
        sb.append("],[");
        append(aggregateTable.aggregations(), sb);
        sb.append("])");
    }

    @Override
    public void visit(TicketTable ticketTable) {
        sb.append("ticketTable(...)");
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

    private static void append(Collection<? extends Aggregation> aggregations, StringBuilder sb) {
        final Iterator<Map.Entry<String, String>> outputNamesAndAggDescriptions =
                AggregationDescriptions.of(aggregations).entrySet().iterator();
        if (outputNamesAndAggDescriptions.hasNext()) {
            append(outputNamesAndAggDescriptions.next(), sb);
        }
        while (outputNamesAndAggDescriptions.hasNext()) {
            append(outputNamesAndAggDescriptions.next(), sb.append(','));
        }
    }

    private static void append(Map.Entry<String, String> outputNameToAggDescription, StringBuilder sb) {
        sb.append(outputNameToAggDescription.getKey())
                .append(" = ")
                .append(outputNameToAggDescription.getValue());
    }
}
