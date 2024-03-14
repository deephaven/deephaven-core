//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.api.ColumnName;
import io.deephaven.qst.table.TableSpec;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;

import java.util.Objects;

final class NamedAdapter implements FieldAdapter, RelNodeAdapter {
    public static ColumnName of(RelDataTypeField field) {
        return ColumnName.of(field.getName());
    }

    private final SqlRootContext rootContext;

    NamedAdapter(SqlRootContext rootContext) {
        this.rootContext = Objects.requireNonNull(rootContext);
    }

    @Override
    public ColumnName output(RelDataTypeField field) {
        return of(field);
    }

    @Override
    public ColumnName input(RexInputRef inputRef, RelDataTypeField inputField) {
        return of(inputField);
    }

    @Override
    public RexNodeFilterAdapter filterAdapter(RelNode parent) {
        return new RexNodeFilterAdapterImpl(parent, this);
    }

    @Override
    public RexNodeExpressionAdapter expressionAdapter(RelNode parent) {
        return new RexNodeExpressionAdapterImpl(parent, this);
    }

    @Override
    public TableSpec table(RelNode node) {
        return RelNodeAdapterNamed.of(rootContext, node);
    }
}
