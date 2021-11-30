package io.deephaven.graphviz;

import io.deephaven.api.SortColumn;
import io.deephaven.api.Strings;
import io.deephaven.api.agg.key.Key.Visitor;
import io.deephaven.api.agg.key.KeyAbsSum;
import io.deephaven.api.agg.key.KeyAvg;
import io.deephaven.api.agg.key.KeyCountDistinct;
import io.deephaven.api.agg.key.KeyDistinct;
import io.deephaven.api.agg.key.KeyFirst;
import io.deephaven.api.agg.key.KeyGroup;
import io.deephaven.api.agg.key.KeyLast;
import io.deephaven.api.agg.key.KeyMax;
import io.deephaven.api.agg.key.KeyMedian;
import io.deephaven.api.agg.key.KeyMin;
import io.deephaven.api.agg.key.KeyPct;
import io.deephaven.api.agg.key.KeySortedFirst;
import io.deephaven.api.agg.key.KeySortedLast;
import io.deephaven.api.agg.key.KeyStd;
import io.deephaven.api.agg.key.KeySum;
import io.deephaven.api.agg.key.KeyUnique;
import io.deephaven.api.agg.key.KeyVar;
import io.deephaven.api.agg.key.KeyWAvg;
import io.deephaven.api.agg.key.KeyWSum;
import io.deephaven.qst.table.AggregationTable;
import io.deephaven.qst.table.AsOfJoinTable;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.ExactJoinTable;
import io.deephaven.qst.table.HeadTable;
import io.deephaven.qst.table.InMemoryAppendOnlyInputTable;
import io.deephaven.qst.table.InMemoryKeyBackedInputTable;
import io.deephaven.qst.table.InputTable;
import io.deephaven.qst.table.Join;
import io.deephaven.qst.table.JoinTable;
import io.deephaven.qst.table.LeftJoinTable;
import io.deephaven.qst.table.NaturalJoinTable;
import io.deephaven.qst.table.ReverseAsOfJoinTable;
import io.deephaven.qst.table.SelectTable;
import io.deephaven.qst.table.SelectableTable;
import io.deephaven.qst.table.SingleAggregationTable;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TableVisitorGeneric;
import io.deephaven.qst.table.TailTable;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.table.TimeTable;
import io.deephaven.qst.table.UpdateTable;
import io.deephaven.qst.table.UpdateViewTable;
import io.deephaven.qst.table.ViewTable;
import io.deephaven.qst.table.WhereTable;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    public void visit(LeftJoinTable leftJoinTable) {
        join("leftJoin", leftJoinTable);
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
    public void visit(WhereTable whereTable) {
        sb.append("where(");
        append(Strings::of, whereTable.filters(), sb);
        sb.append(')');
    }

    @Override
    public void visit(SingleAggregationTable singleAggregationTable) {
        singleAggregationTable.key().walk(new Visitor() {
            @Override
            public void visit(KeyAbsSum absSum) {
                sb.append("absSumBy(");
                append(Strings::of, singleAggregationTable.columns(), sb);
                sb.append(')');
            }

            @Override
            public void visit(KeyCountDistinct countDistinct) {
                sb.append("countDistinctBy(");
                sb.append(countDistinct.countNulls()).append(',');
                append(Strings::of, singleAggregationTable.columns(), sb);
                sb.append(')');
            }

            @Override
            public void visit(KeyDistinct distinct) {
                sb.append("distinctBy(");
                sb.append(distinct.includeNulls()).append(',');
                append(Strings::of, singleAggregationTable.columns(), sb);
                sb.append(')');
            }

            @Override
            public void visit(KeyGroup group) {
                sb.append("groupBy(");
                append(Strings::of, singleAggregationTable.columns(), sb);
                sb.append(')');
            }

            @Override
            public void visit(KeyAvg avg) {
                sb.append("avgBy(");
                append(Strings::of, singleAggregationTable.columns(), sb);
                sb.append(')');
            }

            @Override
            public void visit(KeyFirst first) {
                sb.append("firstBy(");
                append(Strings::of, singleAggregationTable.columns(), sb);
                sb.append(')');
            }

            @Override
            public void visit(KeyLast last) {
                sb.append("lastBy(");
                append(Strings::of, singleAggregationTable.columns(), sb);
                sb.append(')');
            }

            @Override
            public void visit(KeyMax max) {
                sb.append("maxBy(");
                append(Strings::of, singleAggregationTable.columns(), sb);
                sb.append(')');
            }

            @Override
            public void visit(KeyMedian median) {
                sb.append("medianBy(");
                sb.append(median.averageMedian()).append(',');
                append(Strings::of, singleAggregationTable.columns(), sb);
                sb.append(')');
            }

            @Override
            public void visit(KeyMin min) {
                sb.append("minBy(");
                append(Strings::of, singleAggregationTable.columns(), sb);
                sb.append(')');
            }

            @Override
            public void visit(KeyPct pct) {
                sb.append("pctBy(");
                sb.append(pct.percentile()).append(',');
                sb.append(pct.averageMedian()).append(',');
                append(Strings::of, singleAggregationTable.columns(), sb);
                sb.append(')');
            }

            @Override
            public void visit(KeySortedFirst sortedFirst) {
                sb.append("sortedFirstBy([");
                append(Strings::of, sortedFirst.columns().stream().map(SortColumn::column).collect(Collectors.toList()),
                        sb);
                sb.append("],");
                append(Strings::of, singleAggregationTable.columns(), sb);
                sb.append(')');
            }

            @Override
            public void visit(KeySortedLast sortedLast) {
                sb.append("sortedLastBy([");
                append(Strings::of, sortedLast.columns().stream().map(SortColumn::column).collect(Collectors.toList()),
                        sb);
                sb.append("],");
                append(Strings::of, singleAggregationTable.columns(), sb);
                sb.append(')');
            }

            @Override
            public void visit(KeyStd std) {
                sb.append("stdBy(");
                append(Strings::of, singleAggregationTable.columns(), sb);
                sb.append(')');
            }

            @Override
            public void visit(KeySum sum) {
                sb.append("sumBy(");
                append(Strings::of, singleAggregationTable.columns(), sb);
                sb.append(')');
            }

            @Override
            public void visit(KeyUnique unique) {
                sb.append("uniqueBy(");
                sb.append(unique.includeNulls()).append(',');
                append(Strings::of, singleAggregationTable.columns(), sb);
                sb.append(')');
            }

            @Override
            public void visit(KeyWAvg wAvg) {
                sb.append("wavgBy(");
                sb.append(wAvg.weight().name()).append(',');
                append(Strings::of, singleAggregationTable.columns(), sb);
                sb.append(')');
            }

            @Override
            public void visit(KeyWSum wSum) {
                sb.append("wsumBy(");
                sb.append(wSum.weight().name()).append(',');
                append(Strings::of, singleAggregationTable.columns(), sb);
                sb.append(')');
            }

            @Override
            public void visit(KeyVar var) {
                sb.append("varBy(");
                append(Strings::of, singleAggregationTable.columns(), sb);
                sb.append(')');
            }
        });
    }

    @Override
    public void visit(AggregationTable aggregationTable) {
        // TODO(deephaven-core#1116): Add labeling, or structuring, for qst graphviz aggregations
        sb.append("groupBy([");
        append(Strings::of, aggregationTable.columns(), sb);
        sb.append("],[ todo ])");
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
}
