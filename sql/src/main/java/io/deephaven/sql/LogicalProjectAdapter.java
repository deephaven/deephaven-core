//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
import io.deephaven.api.expression.Expression;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.ViewTable;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

final class LogicalProjectAdapter {

    public static TableSpec namedTable(SqlRootContext rootContext, LogicalProject project) {
        return of(rootContext, project, rootContext.namedAdapter());
    }

    public static TableSpec indexTable(SqlRootContext rootContext, LogicalProject project, IndexRef outputIndexRef) {
        return of(rootContext, project, outputIndexRef);
    }

    private static TableSpec of(SqlRootContext rootContext, LogicalProject project,
            OutputFieldAdapter outputFieldAdapter) {
        // We need to make sure input names don't overlap w/ the output names.
        // This is easiest to do if the parent is in index space.
        final IndexRef indexInputRef = rootContext.createIndexRef(Prefix.PROJECT, project);
        final TableSpec indexParent = RelNodeAdapterIndexRef.of(rootContext, project.getInput(), indexInputRef);
        // SQLTODO(view-vs-select)
        // When should we use view vs select? Should it be user configurable (hintable)?
        final ViewTable.Builder builder = ViewTable.builder().parent(indexParent);
        final RexNodeExpressionAdapter adapter = indexInputRef.expressionAdapter(project);
        for (RelDataTypeField outputField : project.getRowType().getFieldList()) {
            final RexNode projection = project.getProjects().get(outputField.getIndex());
            final Expression expression = adapter.expression(projection);
            // <output column name> = <expression>
            final ColumnName outputColumnName = outputFieldAdapter.output(outputField);
            builder.addColumns(Selectable.of(outputColumnName, expression));
        }
        return builder.build();
    }
}
