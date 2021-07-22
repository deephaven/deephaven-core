package io.deephaven.graphviz;

import guru.nidi.graphviz.attribute.Label;
import guru.nidi.graphviz.attribute.Shape;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.model.MutableNode;
import io.deephaven.qst.table.LabeledTable;
import io.deephaven.qst.table.LabeledTables;
import io.deephaven.qst.table.ParentsVisitor;
import io.deephaven.qst.table.Table;

import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.function.Consumer;

import static guru.nidi.graphviz.model.Factory.mutGraph;
import static guru.nidi.graphviz.model.Factory.mutNode;
import static guru.nidi.graphviz.model.Factory.to;

class GraphVizBuilder {

    static MutableGraph of(Iterable<Table> tables) {
        NodesBuilder consumer = new NodesBuilder();
        ParentsVisitor.depthFirstWalk(tables, consumer);
        MutableGraph graph = mutGraph().setDirected(true);
        for (MutableNode node : consumer.identifiers.values()) {
            graph.add(node);
        }
        AppendLinks.ofAll(consumer.identifiers);
        return graph;
    }

    static MutableGraph of(LabeledTables tables) {
        NodesBuilder consumer = new NodesBuilder();
        ParentsVisitor.depthFirstWalk(tables.tables(), consumer);
        MutableGraph graph = mutGraph().setDirected(true);
        for (MutableNode node : consumer.identifiers.values()) {
            graph.add(node);
        }
        AppendLinks.ofAll(consumer.identifiers);

        int argIndex = 0;
        for (LabeledTable e : tables) {
            String label = e.label();
            Table table = e.table();

            MutableNode argNode = mutNode(Label.of(label)).add(Shape.NOTE);
            graph.add(argNode);

            MutableNode other = Objects.requireNonNull(consumer.identifiers.get(table));
            argNode.addLink(to(other).with(Label.of(Integer.toString(argIndex))));

            ++argIndex;
        }

        return graph;
    }


    private static class NodesBuilder implements Consumer<Table> {

        private final LinkedHashMap<Table, MutableNode> identifiers = new LinkedHashMap<>();
        private int current;

        @Override
        public final void accept(Table table) {
            MutableNode node =
                mutNode(String.format("op_%d", current++)).add(Label.of(LabelBuilder.of(table)));
            if (identifiers.put(table, node) != null) {
                throw new IllegalStateException();
            }
        }
    }
}
