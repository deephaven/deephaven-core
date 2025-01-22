//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.impl;

import org.apache.parquet.column.ColumnDescriptor;

public final class ColumnDescriptorUtil {
    /**
     * A more thorough check of {@link ColumnDescriptor} equality. In addition to
     * {@link ColumnDescriptor#equals(Object)} which only checks the {@link ColumnDescriptor#getPath()}, this also
     * checks for the equality of {@link ColumnDescriptor#getPrimitiveType()},
     * {@link ColumnDescriptor#getMaxRepetitionLevel()}, and {@link ColumnDescriptor#getMaxDefinitionLevel()}.
     */
    public static boolean equals(ColumnDescriptor x, ColumnDescriptor y) {
        return x.equals(y)
                && x.getPrimitiveType().equals(y.getPrimitiveType())
                && x.getMaxRepetitionLevel() == y.getMaxRepetitionLevel()
                && x.getMaxDefinitionLevel() == y.getMaxDefinitionLevel();
    }
}
