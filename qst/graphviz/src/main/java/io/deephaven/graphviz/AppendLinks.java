package io.deephaven.graphviz;

import guru.nidi.graphviz.attribute.Label;
import guru.nidi.graphviz.model.MutableNode;
import io.deephaven.qst.table.LinkDescriber;
import io.deephaven.qst.table.LinkDescriber.LinkConsumer;
import io.deephaven.qst.table.TableSpec;

import java.util.Map;
import java.util.Objects;

import static guru.nidi.graphviz.model.Factory.to;

public class AppendLinks implements LinkConsumer {

    /**
     * Add the edges for all the tables, with labels as constructed by {@link LinkDescriber}.
     *
     * @param identifiers the graph nodes
     */
    public static void ofAll(Map<TableSpec, MutableNode> identifiers) {
        for (TableSpec table : identifiers.keySet()) {
            of(identifiers, table);
        }
    }

    /**
     * Add the edges from {@code table} to its dependencies, with labels as constructed by {@link LinkDescriber}.
     *
     * @param identifiers the graph nodes
     * @param table the table
     */
    public static void of(Map<TableSpec, MutableNode> identifiers, TableSpec table) {
        new AppendLinks(identifiers, table).appendLinks();
    }

    private final Map<TableSpec, MutableNode> identifiers;
    private final TableSpec dependent;

    private AppendLinks(Map<TableSpec, MutableNode> identifiers, TableSpec dependent) {
        this.identifiers = Objects.requireNonNull(identifiers);
        this.dependent = Objects.requireNonNull(dependent);
    }

    void appendLinks() {
        dependent.walk(new LinkDescriber(this));
    }

    private MutableNode node(TableSpec t) {
        return Objects.requireNonNull(identifiers.get(t));
    }

    @Override
    public void link(TableSpec table) {
        node(dependent).addLink(node(table));
    }

    @Override
    public void link(TableSpec table, int linkIndex) {
        node(dependent).addLink(to(node(table)).with(Label.of(Integer.toString(linkIndex))));
    }

    @Override
    public void link(TableSpec table, String linkLabel) {
        node(dependent).addLink(to(node(table)).with(Label.of(linkLabel)));
    }
}
