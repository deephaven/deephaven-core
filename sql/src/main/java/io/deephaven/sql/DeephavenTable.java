//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.Objects;

final class DeephavenTable extends AbstractTable {
    private final RelDataType dataType;

    public DeephavenTable(RelDataType dataType) {
        this.dataType = Objects.requireNonNull(dataType);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.copyType(dataType);
    }
}
