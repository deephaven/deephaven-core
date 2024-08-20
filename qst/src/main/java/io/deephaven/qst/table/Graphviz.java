//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.qst.table.LinkDescriber.LinkConsumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public final class Graphviz {
    private static final Pattern LINEBREAK_PATTERN = Pattern.compile("\\R");

    /**
     * Creates a graphviz DOT language string for {@code tableSpec}.
     *
     * <p>
     * Callers can use this for visualization and debugging purposes, but should not depend on the strings being equal
     * from release to release.
     *
     * @param tableSpec the table spec
     * @return the DOT string
     * @see <a href="https://graphviz.org/doc/info/lang.html">DOT</a>
     */
    public static String toDot(TableSpec tableSpec) {
        return toDot(Collections.singleton(tableSpec));
    }

    /**
     * Creates a graphviz DOT language string for {@code tableSpecs}.
     *
     * <p>
     * Callers can use this for visualization and debugging purposes, but should not depend on the strings being equal
     * from release to release.
     *
     * @param tableSpecs the table specs
     * @return the DOT string
     * @see <a href="https://graphviz.org/doc/info/lang.html">DOT</a>
     */
    public static String toDot(Iterable<TableSpec> tableSpecs) {
        final List<TableSpec> specs = ParentsVisitor.postOrderList(tableSpecs);
        // Create a reverse-lookup (instead of relying on List#indexOf)
        final Map<TableSpec, Integer> ids = new HashMap<>(specs.size());
        final StringBuilder sb = new StringBuilder();
        sb.append("digraph {");
        sb.append(System.lineSeparator());
        for (int id = 0; id < specs.size(); ++id) {
            final TableSpec spec = specs.get(id);
            sb.append(String.format("\"op_%d\" [\"label\"=\"%s\"]%n", id, escape(TableLabelVisitor.of(spec))));
            ids.put(spec, id);
        }
        for (int i = 0; i < specs.size(); ++i) {
            final int id = i;
            final TableSpec spec = specs.get(id);
            spec.walk(new LinkDescriber(new LinkConsumer() {
                @Override
                public void link(TableSpec parent) {
                    final int parentId = ids.get(parent);
                    sb.append(String.format("\"op_%d\" -> \"op_%d\"%n", id, parentId));
                }

                @Override
                public void link(TableSpec parent, int linkIndex) {
                    final int parentId = ids.get(parent);
                    sb.append(String.format("\"op_%d\" -> \"op_%d\" [\"label\"=\"%d\"]%n", id, parentId, linkIndex));
                }

                @Override
                public void link(TableSpec parent, String linkLabel) {
                    final int parentId = ids.get(parent);
                    sb.append(String.format("\"op_%d\" -> \"op_%d\" [\"label\"=\"%s\"]%n", id, parentId,
                            escape(linkLabel)));

                }
            }));
        }
        sb.append("}");
        return sb.toString();
    }

    private static String escape(String x) {
        int count = 0;
        while (count < x.length() && x.charAt(x.length() - 1 - count) == '\\') {
            count++;
        }
        // Ending can't be escaped by an odd number of '\'
        final String end = count % 2 == 1 ? "\\" : "";
        final String r1 = x.replace("\"", "\\\"");
        return LINEBREAK_PATTERN.matcher(r1).replaceAll("\\\\n") + end;
    }
}
