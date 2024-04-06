//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.qst.table.TableSpec;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;

import java.util.Objects;

final class RelNodeAdapterNamed implements RelNodeVisitor<TableSpec> {

    public static TableSpec of(SqlRootContext rootContext, RelNode node) {
        return RelNodeVisitor.accept(node, new RelNodeAdapterNamed(rootContext, node));
    }

    private final SqlRootContext rootContext;
    private final RelNode node;

    RelNodeAdapterNamed(SqlRootContext rootContext, RelNode node) {
        this.rootContext = Objects.requireNonNull(rootContext);
        this.node = Objects.requireNonNull(node);
    }

    @Override
    public TableSpec visit(LogicalProject project) {
        return LogicalProjectAdapter.namedTable(rootContext, project);
    }

    @Override
    public TableSpec visit(LogicalUnion union) {
        return LogicalUnionAdapter.of(union, rootContext.namedAdapter());
    }

    @Override
    public TableSpec visit(LogicalSort sort) {
        return LogicalSortAdapter.namedTable(sort, rootContext.namedAdapter());
    }

    @Override
    public TableSpec visit(TableScan scan) {
        throw new IllegalStateException();
    }

    @Override
    public TableSpec visit(LogicalFilter filter) {
        throw new IllegalStateException();
    }

    @Override
    public TableSpec visit(LogicalJoin join) {
        throw new IllegalStateException();
    }

    @Override
    public TableSpec visit(LogicalAggregate aggregate) {
        return LogicalAggregateAdapter.namedTable(rootContext, aggregate);
    }

    @Override
    public TableSpec visit(LogicalValues values) {
        return LogicalValuesAdapter.namedTable(values);
    }

    private TableSpec indexToName(TableSpec table, IndexRef indexRef) {
        return Helper.indexToName(table, node, indexRef);
    }
}
