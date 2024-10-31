//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.api.SortColumn;
import io.deephaven.api.Strings;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;

public class TableLabelVisitor extends TableVisitorGeneric<String> {

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
        return table.walk(new TableLabelVisitor());
    }

    @Override
    public String accept(TableSpec t) {
        // All tables are ImmutableX - we want the non-immutable version of the class, slightly
        // prettier.
        return t.getClass().getSuperclass().getSimpleName();
    }

    @Override
    public String visit(EmptyTable emptyTable) {
        return String.format("empty(%d)", emptyTable.size());
    }

    @Override
    public String visit(TimeTable timeTable) {
        return String.format("time(%s)", timeTable.interval());
    }

    @Override
    public String visit(MergeTable mergeTable) {
        return "merge()";
    }

    @Override
    public String visit(NewTable newTable) {
        return String.format("newTable(%d, %s)", newTable.size(), newTable.header());
    }

    @Override
    public String visit(HeadTable headTable) {
        return "head(" + headTable.size() + ")";
    }

    @Override
    public String visit(TailTable tailTable) {
        return "tail(" + tailTable.size() + ")";
    }

    @Override
    public String visit(SliceTable sliceTable) {
        return String.format("slice(%d,%d)", sliceTable.firstPositionInclusive(), sliceTable.lastPositionExclusive());
    }

    @Override
    public String visit(NaturalJoinTable naturalJoinTable) {
        return join("naturalJoin", naturalJoinTable);
    }

    @Override
    public String visit(ExactJoinTable exactJoinTable) {
        return join("exactJoin", exactJoinTable);
    }

    @Override
    public String visit(JoinTable joinTable) {
        return join("join", joinTable);
    }

    @Override
    public String visit(AsOfJoinTable aj) {
        return String.format("asOfJoin([%s],%s,%s)",
                append(Strings::of, aj.matches()),
                Strings.of(aj.joinMatch()),
                append(Strings::of, aj.additions()));
    }

    @Override
    public String visit(RangeJoinTable rangeJoinTable) {
        return String.format("rangJoin([%s],%s,%s)",
                append(Strings::of, rangeJoinTable.exactMatches()),
                Strings.of(rangeJoinTable.rangeMatch()),
                Strings.ofAggregations(rangeJoinTable.aggregations()));
    }

    @Override
    public String visit(ViewTable viewTable) {
        return selectable("view", viewTable);
    }

    @Override
    public String visit(SelectTable selectTable) {
        return selectable("select", selectTable);
    }

    @Override
    public String visit(UpdateViewTable updateViewTable) {
        return selectable("updateView", updateViewTable);
    }

    @Override
    public String visit(UpdateTable updateTable) {
        return selectable("update", updateTable);
    }

    @Override
    public String visit(LazyUpdateTable lazyUpdateTable) {
        return selectable("lazyUpdate", lazyUpdateTable);
    }

    @Override
    public String visit(WhereTable whereTable) {
        return String.format("where(%s)", Strings.of(whereTable.filter()));
    }

    @Override
    public String visit(AggregateAllTable aggregateAllTable) {
        return String.format("aggAllBy(%s,%s)",
                aggregateAllTable.spec().description(),
                append(Strings::of, aggregateAllTable.groupByColumns()));
    }

    @Override
    public String visit(AggregateTable aggregateTable) {
        return String.format("aggBy([%s],%s)",
                append(Strings::of, aggregateTable.groupByColumns()),
                Strings.ofAggregations(aggregateTable.aggregations()));
    }

    @Override
    public String visit(TicketTable ticketTable) {
        return String.format("ticketTable(%s)", new String(ticketTable.ticket(), StandardCharsets.UTF_8));
    }

    @Override
    public String visit(InputTable inputTable) {
        return inputTable.walk(new InputTable.Visitor<String>() {
            @Override
            public String visit(InMemoryAppendOnlyInputTable inMemoryAppendOnly) {
                return "InMemoryAppendOnlyInputTable(...)";
            }

            @Override
            public String visit(InMemoryKeyBackedInputTable inMemoryKeyBacked) {
                return "InMemoryKeyBackedInputTable(...)";
            }

            @Override
            public String visit(BlinkInputTable blinkInputTable) {
                return "BlinkInputTable(...)";
            }
        });
    }

    @Override
    public String visit(SelectDistinctTable selectDistinctTable) {
        return String.format("selectDistinct(%s)", append(Strings::of, selectDistinctTable.columns()));
    }

    @Override
    public String visit(UpdateByTable updateByTable) {
        // TODO(deephaven-core#1116): Add labeling, or structuring, for qst graphviz aggregations
        return String.format("updateBy([%s])", append(Strings::of, updateByTable.groupByColumns()));
    }

    @Override
    public String visit(UngroupTable ungroupTable) {
        return String.format("ungroup(%b,[%s])",
                ungroupTable.nullFill(),
                append(Strings::of, ungroupTable.ungroupColumns()));
    }

    @Override
    public String visit(DropColumnsTable dropColumnsTable) {
        return String.format("dropColumns([%s])", append(Strings::of, dropColumnsTable.dropColumns()));
    }

    @Override
    public String visit(ReverseTable reverseTable) {
        return "reverse()";
    }

    @Override
    public String visit(SortTable sortTable) {
        return String.format("sort([%s])", append(TableLabelVisitor::toString, sortTable.columns()));
    }

    @Override
    public String visit(SnapshotTable snapshotTable) {
        return "snapshot()";
    }

    @Override
    public String visit(MultiJoinTable multiJoinTable) {
        return "multiJoin()";
    }

    private String join(String name, Join j) {
        return String.format("%s([%s],[%s])",
                name,
                append(Strings::of, j.matches()),
                append(Strings::of, j.additions()));
    }

    private String selectable(String name, SelectableTable j) {
        return String.format("%s(%s)", name, append(Strings::of, j.columns()));
    }

    private static <T> String append(Function<T, String> f, Collection<T> c) {
        StringBuilder sb = new StringBuilder();
        Iterator<T> it = c.iterator();
        if (it.hasNext()) {
            sb.append(f.apply(it.next()));
        }
        while (it.hasNext()) {
            sb.append(',').append(f.apply(it.next()));
        }
        return sb.toString();
    }

    private static String toString(SortColumn sort) {
        return String.format("%s(%s)", sort.order(), sort.column().name());
    }
}
