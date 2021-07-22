package io.deephaven.graphviz;

import guru.nidi.graphviz.attribute.Label;
import guru.nidi.graphviz.model.MutableNode;
import io.deephaven.qst.table.LinkDescriber;
import io.deephaven.qst.table.LinkDescriber.LinkConsumer;
import io.deephaven.qst.table.Table;

import java.util.Map;
import java.util.Objects;

import static guru.nidi.graphviz.model.Factory.to;

class AppendLinks implements LinkConsumer {

    static void ofAll(Map<Table, MutableNode> identifiers) {
        for (Table table : identifiers.keySet()) {
            of(identifiers, table);
        }
    }

    static void of(Map<Table, MutableNode> identifiers, Table table) {
        new AppendLinks(identifiers, table).appendLinks();
    }

    private final Map<Table, MutableNode> identifiers;
    private final Table dependent;

    private AppendLinks(Map<Table, MutableNode> identifiers, Table dependent) {
        this.identifiers = Objects.requireNonNull(identifiers);
        this.dependent = Objects.requireNonNull(dependent);
    }

    void appendLinks() {
        dependent.walk(new LinkDescriber(this));
    }

    private MutableNode node(Table t) {
        return Objects.requireNonNull(identifiers.get(t));
    }

    @Override
    public void link(Table table) {
        node(dependent).addLink(node(table));
    }

    @Override
    public void link(Table table, int linkIndex) {
        node(dependent).addLink(to(node(table)).with(Label.of(Integer.toString(linkIndex))));
    }

    @Override
    public void link(Table table, String linkLabel) {
        node(dependent).addLink(to(node(table)).with(Label.of(linkLabel)));
    }
}
