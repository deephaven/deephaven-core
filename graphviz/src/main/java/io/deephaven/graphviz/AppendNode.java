package io.deephaven.graphviz;

import io.deephaven.qst.table.Table;

import java.util.Map;
import java.util.Objects;

class AppendNode {

    static void ofAll(Map<Table, String> identifiers, StringBuilder sb) {
        LabelBuilder labelBuilder = new LabelBuilder(sb);
        for (Table table : identifiers.keySet()) {
            String id = Objects.requireNonNull(identifiers.get(table));
            sb.append(id).append(" [label=\"");
            table.walk(labelBuilder);
            sb.append("\"]").append(System.lineSeparator());
        }
    }

    static void of(Map<Table, String> identifiers, StringBuilder sb, Table table) {
        LabelBuilder labelBuilder = new LabelBuilder(sb);
        String id = Objects.requireNonNull(identifiers.get(table));
        sb.append(id).append(" [label=\"");
        table.walk(labelBuilder);
        sb.append("\"]").append(System.lineSeparator());
    }

    // Note: we may eventually want to add more graph-structure depending on Table type.
    // For example, we may want to describe selects/filters graphically instead of textually.
    // In that case, we can have AppendNode implement TableVisitor.
}
