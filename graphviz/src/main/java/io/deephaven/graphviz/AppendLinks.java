package io.deephaven.graphviz;

import io.deephaven.qst.table.LinkDescriber;
import io.deephaven.qst.table.LinkDescriber.LinkConsumer;
import io.deephaven.qst.table.Table;

import java.util.Map;
import java.util.Objects;

class AppendLinks implements LinkConsumer {

    static void ofAll(Map<Table, String> identifiers, StringBuilder sb) {
        for (Table table : identifiers.keySet()) {
            of(identifiers, sb, table);
        }
    }

    static void of(Map<Table, String> identifiers, StringBuilder sb, Table table) {
        new AppendLinks(identifiers, sb, table).appendLinks();
    }

    private final Map<Table, String> identifiers;
    private final StringBuilder sb;
    private final Table dependent;

    private AppendLinks(Map<Table, String> identifiers, StringBuilder sb, Table dependent) {
        this.identifiers = Objects.requireNonNull(identifiers);
        this.sb = Objects.requireNonNull(sb);
        this.dependent = Objects.requireNonNull(dependent);
    }

    void appendLinks() {
        dependent.walk(new LinkDescriber(this));
    }

    private String id(Table t) {
        return Objects.requireNonNull(identifiers.get(t));
    }

    @Override
    public void link(Table table) {
        sb.append(id(dependent)).append(" -> ").append(id(table)).append(System.lineSeparator());
    }

    @Override
    public void link(Table table, int linkIndex) {
        sb.append(id(dependent)).append(" -> ").append(id(table)).append(" [label=\"")
            .append(linkIndex).append("\"]").append(System.lineSeparator());
    }

    @Override
    public void link(Table table, String linkLabel) {
        sb.append(id(dependent)).append(" -> ").append(id(table)).append(" [label=\"")
            .append(linkLabel).append("\"]").append(System.lineSeparator());
    }
}
