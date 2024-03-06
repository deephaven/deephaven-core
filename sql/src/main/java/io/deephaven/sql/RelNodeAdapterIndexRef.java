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

final class RelNodeAdapterIndexRef implements RelNodeVisitor<TableSpec> {

    public static TableSpec of(SqlRootContext rootContext, RelNode node, IndexRef indexRef) {
        return RelNodeVisitor.accept(node, new RelNodeAdapterIndexRef(rootContext, node, indexRef));
    }

    private final SqlRootContext rootContext;
    private final RelNode node;
    private final IndexRef indexRef;

    RelNodeAdapterIndexRef(SqlRootContext rootContext, RelNode node, IndexRef indexRef) {
        this.rootContext = Objects.requireNonNull(rootContext);
        this.node = Objects.requireNonNull(node);
        this.indexRef = Objects.requireNonNull(indexRef);
    }

    @Override
    public TableSpec visit(LogicalProject project) {
        return LogicalProjectAdapter.indexTable(rootContext, project, indexRef);
    }

    @Override
    public TableSpec visit(LogicalUnion union) {
        return LogicalUnionAdapter.of(union, indexRef);
    }

    @Override
    public TableSpec visit(LogicalSort sort) {
        return LogicalSortAdapter.indexTable(sort, indexRef);
    }

    @Override
    public TableSpec visit(TableScan scan) {
        final TableSpec spec = rootContext.scope()
                .table(scan.getTable().getQualifiedName())
                .map(TableInformation::spec)
                .orElseThrow();
        return nameToIndex(spec);
    }

    @Override
    public TableSpec visit(LogicalFilter filter) {
        return LogicalFilterAdapter.indexTable(filter, indexRef);
    }

    @Override
    public TableSpec visit(LogicalJoin join) {
        return LogicalJoinAdapter.indexTable(rootContext, join, indexRef);
    }

    @Override
    public TableSpec visit(LogicalAggregate aggregate) {
        return LogicalAggregateAdapter.indexTable(rootContext, aggregate, indexRef);
    }

    @Override
    public TableSpec visit(LogicalValues values) {
        return nameToIndex(LogicalValuesAdapter.namedTable(values));
    }

    private TableSpec nameToIndex(TableSpec table) {
        return Helper.nameToIndex(table, node, indexRef);
    }
}
