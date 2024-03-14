//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.ViewTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;

final class Helper {

    public static TableSpec indexToName(TableSpec table, RelNode node, IndexRef indexRef) {
        final ViewTable.Builder builder = ViewTable.builder().parent(table);
        for (RelDataTypeField outputField : node.getRowType().getFieldList()) {
            final ColumnName newName = NamedAdapter.of(outputField);
            final ColumnName existing = indexRef.output(outputField);
            builder.addColumns(Selectable.of(newName, existing));
        }
        return builder.build();
    }

    public static TableSpec nameToIndex(TableSpec table, RelNode node, IndexRef indexRef) {
        final ViewTable.Builder builder = ViewTable.builder().parent(table);
        for (RelDataTypeField outputField : node.getRowType().getFieldList()) {
            final ColumnName newName = indexRef.output(outputField);
            final ColumnName existing = NamedAdapter.of(outputField);
            builder.addColumns(Selectable.of(newName, existing));
        }
        return builder.build();
    }

    public static RelDataTypeField inputField(RelNode node, final int fieldIndex) {
        int ix = fieldIndex;
        for (RelNode input : node.getInputs()) {
            final int fieldCount = input.getRowType().getFieldCount();
            if (ix >= fieldCount) {
                ix -= fieldCount;
                continue;
            }
            return input.getRowType().getFieldList().get(ix);
        }
        throw new IllegalStateException();
    }

    public static ColumnName inputColumnName(RelNode node, final int inputIndex, FieldAdapter adapter) {
        final RelDataTypeField inputField = inputField(node, inputIndex);
        final RexInputRef inputRef = new RexInputRef(inputIndex, inputField.getType());
        return adapter.input(inputRef, inputField);
    }

    public static ColumnName outputColumnName(RelNode node, final int outputIndex, FieldAdapter adapter) {
        return adapter.output(node.getRowType().getFieldList().get(outputIndex));
    }
}
