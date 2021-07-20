package io.deephaven.graphviz;

import io.deephaven.qst.table.LabeledTable;
import io.deephaven.qst.table.LabeledTables;
import io.deephaven.qst.table.ParentsVisitor;
import io.deephaven.qst.table.Table;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Consumer;

class GraphVizBuilder {

    static String of(Iterable<Table> tables) {
        IdentifiersBuilder consumer = new IdentifiersBuilder();
        ParentsVisitor.depthFirstWalk(tables, consumer);
        StringBuilder sb = new StringBuilder();

        sb.append("digraph {").append(System.lineSeparator());
        AppendNode.ofAll(consumer.identifiers, sb);
        AppendLinks.ofAll(consumer.identifiers, sb);
        sb.append("}");

        return sb.toString();
    }

    static String of(LabeledTables tables) {
        IdentifiersBuilder consumer = new IdentifiersBuilder();
        ParentsVisitor.depthFirstWalk(tables.tables(), consumer);
        StringBuilder sb = new StringBuilder();

        sb.append("digraph {").append(System.lineSeparator());
        AppendNode.ofAll(consumer.identifiers, sb);
        AppendLinks.ofAll(consumer.identifiers, sb);

        int argIndex = 0;

        for (LabeledTable e : tables) {
            String label = e.label();
            Table table = e.table();
            String id = Objects.requireNonNull(consumer.identifiers.get(table));

            sb.append("arg_").append(argIndex).append(" [shape=note label=\"").append(label)
                .append("\"]").append(System.lineSeparator());
            sb.append("arg_").append(argIndex).append(" -> ").append(id).append(" [label=\"")
                .append(argIndex).append("\"]").append(System.lineSeparator());
            ++argIndex;
        }

        sb.append("}");

        return sb.toString();
    }


    private static class IdentifiersBuilder implements Consumer<Table> {

        private final LinkedHashMap<Table, String> identifiers = new LinkedHashMap<>();
        private int current;

        @Override
        public final void accept(Table table) {
            if (identifiers.put(table, String.format("op_%d", current++)) != null) {
                throw new IllegalStateException();
            }
        }
    }
}
