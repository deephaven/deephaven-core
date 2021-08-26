package io.deephaven.graphviz;

import guru.nidi.graphviz.attribute.Label;
import guru.nidi.graphviz.attribute.Shape;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.model.MutableNode;
import io.deephaven.qst.table.LabeledTable;
import io.deephaven.qst.table.LabeledTables;
import io.deephaven.qst.table.ParentsVisitor;
import io.deephaven.qst.table.TableSpec;

import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.function.Consumer;

import static guru.nidi.graphviz.model.Factory.mutGraph;
import static guru.nidi.graphviz.model.Factory.mutNode;
import static guru.nidi.graphviz.model.Factory.to;

public class GraphVizBuilder {

    /**
     * Creates a directed {@link MutableGraph mutable graph} from the given {@code tables}. Tables that are equal will
     * be represented by the same node in the graph. Dependencies will be represented as directed edges, from dependee
     * to dependency.
     *
     * @param tables the tables
     * @return the graph
     */
    public static MutableGraph of(Iterable<TableSpec> tables) {
        NodesBuilder consumer = new NodesBuilder();
        ParentsVisitor.postOrderList(tables).forEach(consumer);
        MutableGraph graph = mutGraph().setDirected(true);
        for (MutableNode node : consumer.identifiers.values()) {
            graph.add(node);
        }
        AppendLinks.ofAll(consumer.identifiers);
        return graph;
    }

    /**
     * Creates a directed {@link MutableGraph mutable graph} from the given {@code tables}. Tables that are equal will
     * be represented by the same node in the graph. Dependencies will be represented as directed edges, from dependee
     * to dependency. Labels will be represented as nodes with directed edges towards the respective table node.
     *
     * @param tables the tables
     * @return the graph
     */
    public static MutableGraph of(LabeledTables tables) {
        NodesBuilder consumer = new NodesBuilder();
        ParentsVisitor.postOrderList(tables.tables()).forEach(consumer);
        MutableGraph graph = mutGraph().setDirected(true);
        for (MutableNode node : consumer.identifiers.values()) {
            graph.add(node);
        }
        AppendLinks.ofAll(consumer.identifiers);

        for (LabeledTable e : tables) {
            String label = e.label();
            TableSpec table = e.table();

            MutableNode argNode = mutNode(Label.of(label)).add(Shape.NOTE);
            graph.add(argNode);

            MutableNode other = Objects.requireNonNull(consumer.identifiers.get(table));
            argNode.addLink(to(other));
        }

        return graph;
    }


    private static class NodesBuilder implements Consumer<TableSpec> {

        private final LinkedHashMap<TableSpec, MutableNode> identifiers = new LinkedHashMap<>();
        private int current;

        @Override
        public final void accept(TableSpec table) {
            MutableNode node =
                    mutNode(String.format("op_%d", current++)).add(Label.of(LabelBuilder.of(table)));
            if (identifiers.put(table, node) != null) {
                throw new IllegalStateException();
            }
        }
    }
}
