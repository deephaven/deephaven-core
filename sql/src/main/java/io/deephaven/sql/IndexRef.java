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

class IndexRef implements FieldAdapter, RelNodeAdapter {

    private static String columnName(String prefix, int index) {
        return prefix + index;
    }

    private final SqlRootContext rootContext;
    private final String columnNamePrefix;
    private final int shift;

    IndexRef(SqlRootContext rootContext, String columnNamePrefix, int shift) {
        this.rootContext = Objects.requireNonNull(rootContext);
        this.columnNamePrefix = Objects.requireNonNull(columnNamePrefix);
        this.shift = shift;
    }

    @Override
    public ColumnName output(RelDataTypeField field) {
        return ColumnName.of(columnName(columnNamePrefix, shift + field.getIndex()));
    }

    @Override
    public ColumnName input(RexInputRef inputRef, RelDataTypeField inputField) {
        return ColumnName.of(columnName(columnNamePrefix, shift + inputRef.getIndex()));
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
        return RelNodeAdapterIndexRef.of(rootContext, node, this);
    }

    public IndexRef shifted(int shift) {
        return new IndexRef(rootContext, columnNamePrefix, this.shift + shift);
    }
}
