//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;

/**
 * This is an improvement on {@link RelShuttle} that visitors to return any type.
 *
 * @param <T> the type
 */
interface RelNodeVisitor<T> {

    static <T> T accept(RelNode node, RelNodeVisitor<T> visitor) {
        final RelNodeVisitorAdapter<T> adapter = new RelNodeVisitorAdapter<>(visitor);
        node.accept(adapter);
        return adapter.out();
    }

    T visit(LogicalProject project);

    T visit(LogicalUnion union);

    T visit(LogicalSort sort);

    T visit(TableScan scan);

    T visit(LogicalFilter filter);

    T visit(LogicalJoin join);

    T visit(LogicalAggregate aggregate);

    T visit(LogicalValues values);
}
