//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import org.apache.calcite.rel.RelNode;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;

final class SqlRootContext {

    public static SqlRootContext of(RelNode root, Scope scope) {
        return new SqlRootContext(root, encounterOrder(root), scope);
    }

    private final RelNode root;
    private final Map<RelNode, Integer> repeatableId;
    private final Scope scope;

    private SqlRootContext(RelNode root, Map<RelNode, Integer> repeatableId, Scope scope) {
        this.root = Objects.requireNonNull(root);
        this.repeatableId = Objects.requireNonNull(repeatableId);
        this.scope = Objects.requireNonNull(scope);
    }

    public RelNode root() {
        return root;
    }

    public Scope scope() {
        return scope;
    }

    public NamedAdapter namedAdapter() {
        return new NamedAdapter(this);
    }

    public IndexRef createIndexRef(Prefix context, RelNode node) {
        // Calcite RelNode#getId is distinct for every relational expression that is created, even if the same SQL query
        // string is parsed again. From a unit testing perspective, and a server cacheability perspective, we would
        // prefer to assign consistent (internal) column names for the same SQL query string.
        final String columnPrefix = context.prefix() + repeatableId(node) + "_";
        return new IndexRef(this, columnPrefix, 0);
    }

    private int repeatableId(RelNode node) {
        final Integer id = repeatableId.get(node);
        if (id == null) {
            throw new IllegalStateException(
                    "Unexpected error. Trying to get repeatable id from RelNode, but RelNode was not seen in #encounterOrder. Either the logic in #encounterOrder is not exhaustive, or this SqlRootContext is being used in an incorrect context.");
        }
        return id;
    }

    private static Map<RelNode, Integer> encounterOrder(RelNode root) {
        // Note: the specific traversal pattern is not important, it's just important that there is some repeatable
        // pattern where we can assign a consistent id based on the rel node structures.
        int encounterOrder = 0;
        final Map<RelNode, Integer> order = new HashMap<>();
        final Queue<RelNode> toVisit = new ArrayDeque<>();
        toVisit.add(root);
        RelNode current;
        while ((current = toVisit.poll()) != null) {
            order.put(current, encounterOrder++);
            toVisit.addAll(current.getInputs());
        }
        return order;
    }
}
