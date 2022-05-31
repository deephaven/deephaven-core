package io.deephaven.graphviz;

import io.deephaven.api.Strings;
import io.deephaven.qst.table.AggregateAllByTable;
import io.deephaven.qst.table.AggregationTable;
import io.deephaven.qst.table.AsOfJoinTable;
import io.deephaven.qst.table.CountByTable;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.ExactJoinTable;
import io.deephaven.qst.table.HeadTable;
import io.deephaven.qst.table.InMemoryAppendOnlyInputTable;
import io.deephaven.qst.table.InMemoryKeyBackedInputTable;
import io.deephaven.qst.table.InputTable;
import io.deephaven.qst.table.Join;
import io.deephaven.qst.table.JoinTable;
import io.deephaven.qst.table.NaturalJoinTable;
import io.deephaven.qst.table.ReverseAsOfJoinTable;
import io.deephaven.qst.table.SelectDistinctTable;
import io.deephaven.qst.table.SelectTable;
import io.deephaven.qst.table.SelectableTable;
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
    public void visit(AggregateAllByTable aggAllByTable) {
        sb.append("aggAllBy(");
        sb.append(aggAllByTable.spec()).append(',');
        append(Strings::of, aggAllByTable.groupByColumns(), sb);
        sb.append(')');
    }

    @Override
    public void visit(AggregationTable aggregationTable) {
        // TODO(deephaven-core#1116): Add labeling, or structuring, for qst graphviz aggregations
        sb.append("aggBy([");
        append(Strings::of, aggregationTable.groupByColumns(), sb);
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

    @Override
    public void visit(SelectDistinctTable selectDistinctTable) {
        sb.append("selectDistinct(");
        append(Strings::of, selectDistinctTable.groupByColumns(), sb);
        sb.append(')');
    }

    @Override
    public void visit(CountByTable countByTable) {
        sb.append("countBy(").append(countByTable.countName()).append(',');
        append(Strings::of, countByTable.groupByColumns(), sb);
        sb.append(')');
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
