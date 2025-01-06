//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;

import java.util.Objects;

final class RelNodeVisitorAdapter<T> implements RelShuttle {

    private final RelNodeVisitor<T> visitor;
    private T out;

    public RelNodeVisitorAdapter(RelNodeVisitor<T> visitor) {
        this.visitor = Objects.requireNonNull(visitor);
    }

    public T out() {
        return Objects.requireNonNull(out);
    }

    @Override
    public RelNode visit(TableScan scan) {
        out = visitor.visit(scan);
        return scan;
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
        // SQLTODO(custom-sources)
        //
        // It would be good to be able to source tables from other places, besides just ones in the global scope. For
        // example:
        // SELECT * FROM parquet('/data/my-dataset.parquet')
        // SELECT * FROM csv('/my/test.csv')
        // SELECT * FROM uri('dh://server/scope/table_name')
        // SELECT * FROM uri('csv:///path/to/the.csv')
        // SELECT * FROM time_table("00:00:01")
        //
        // Potentially related to design decisions around SQLTODO(catalog-reader-implementation)
        throw new UnsupportedSqlOperation("SQLTODO(custom-sources)", TableFunctionScan.class);
    }

    @Override
    public RelNode visit(LogicalValues values) {
        out = visitor.visit(values);
        return values;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        out = visitor.visit(filter);
        return filter;
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        throw new UnsupportedSqlOperation(LogicalCalc.class);
    }

    @Override
    public RelNode visit(LogicalProject project) {
        out = visitor.visit(project);
        return project;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        out = visitor.visit(join);
        return join;
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        throw new UnsupportedSqlOperation(LogicalCorrelate.class);
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        out = visitor.visit(union);
        return union;
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        // SQLTODO(logical-intersect)
        // table.whereIn
        throw new UnsupportedSqlOperation("SQLTODO(logical-intersect)", LogicalIntersect.class);
    }

    @Override
    public RelNode visit(LogicalMinus minus) {
        // SQLTODO(logical-minus)
        // table.whereNotIn
        throw new UnsupportedSqlOperation("SQLTODO(logical-minus)", LogicalMatch.class);
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        out = visitor.visit(aggregate);
        return aggregate;
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        throw new UnsupportedSqlOperation(LogicalMatch.class);
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        out = visitor.visit(sort);
        return sort;
    }

    @Override
    public RelNode visit(LogicalExchange exchange) {
        throw new UnsupportedSqlOperation(LogicalExchange.class);
    }

    @Override
    public RelNode visit(LogicalTableModify modify) {
        throw new UnsupportedSqlOperation(LogicalTableModify.class);
    }

    @Override
    public RelNode visit(RelNode other) {
        throw new UnsupportedSqlOperation(RelNode.class);
    }
}
