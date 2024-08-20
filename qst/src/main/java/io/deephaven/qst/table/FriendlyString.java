//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import java.util.List;
import java.util.stream.Collectors;

public final class FriendlyString {

    /**
     * Constructs a "friendly", depth-limited, recursive string of the given {@code tableSpec}. The depth visited is
     * limited by 10. This is a non de-duplicating construction, and is most useful for smaller sized table specs.
     * Callers that prefer a de-duplicated construction that works efficiently with larger tables may prefer
     * {@link Graphviz#toDot(TableSpec)}.
     *
     * <p>
     * Callers can use this for visualization and debugging purposes, but should not depend on the strings being equal
     * from release to release.
     *
     * @param tableSpec the table spec
     * @return the "friendly" string
     */
    public static String of(TableSpec tableSpec) {
        return of(tableSpec, 10);
    }

    /**
     * Constructs a "friendly" string of the given {@code tableSpec}. The depth visited is limited by
     * {@code maxAncestorDepth}. This is a non de-duplicating construction, and is most useful for smaller sized table
     * specs. Callers that prefer a de-duplicated construction that works efficiently with larger tables may prefer
     * {@link Graphviz#toDot(TableSpec)}.
     * 
     * <p>
     * Callers can use this for visualization and debugging purposes, but should not depend on the strings being equal
     * from release to release.
     *
     * @param tableSpec the table spec
     * @param maxAncestorDepth the maximum number of ancestors to visit
     * @return the "friendly" string
     */
    public static String of(TableSpec tableSpec, int maxAncestorDepth) {
        final StringBuilder sb = new StringBuilder();
        // Note: this will re-visit TableSpec that may have already been printed out, so it's not a de-duplicating
        // representation like graph-based string formats may present (for example, graphviz format).
        final List<TableSpec> parents = ParentsVisitor.getParents(tableSpec).collect(Collectors.toList());
        if (maxAncestorDepth == 0) {
            if (!parents.isEmpty()) {
                sb.append("...");
                sb.append(System.lineSeparator());
            }
        } else {
            final boolean hasMultipleParents = parents.size() > 1;
            if (hasMultipleParents) {
                sb.append('[');
                sb.append(System.lineSeparator());
            }
            for (TableSpec parent : parents) {
                sb.append(of(parent, maxAncestorDepth - 1));
                if (hasMultipleParents) {
                    sb.append(',');
                }
                sb.append(System.lineSeparator());
            }
            if (hasMultipleParents) {
                sb.append(']');
                sb.append(System.lineSeparator());
            }
            if (!parents.isEmpty()) {
                sb.append('.');
            }
        }
        sb.append(TableLabelVisitor.of(tableSpec));
        return sb.toString();
    }
}
