//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.util.annotations.InternalUseOnly;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.TypeVisitor;

@InternalUseOnly
public final class ParquetTotalColumns {

    public static int of(Type type) {
        final TotalColumnsVisitor visitor = new TotalColumnsVisitor();
        type.accept(visitor);
        return visitor.out;
    }

    public static int of(@SuppressWarnings("unused") PrimitiveType primitiveType) {
        return 1;
    }

    public static int of(GroupType groupType) {
        return groupType.getFields().stream().mapToInt(ParquetTotalColumns::of).sum();
    }

    public static int of(MessageType messageType) {
        final int numColumns = of((GroupType) messageType);
        // same size as messageType.getColumns().size(), but this is cheaper.
        final int numPaths = messageType.getPaths().size();
        if (numColumns != numPaths) {
            throw new IllegalStateException(
                    String.format("Inconsistent sizes, numColumns=%d, numPaths=%d", numColumns, numPaths));
        }
        return numColumns;
    }

    private static class TotalColumnsVisitor implements TypeVisitor {
        private int out;

        @Override
        public void visit(GroupType groupType) {
            out = of(groupType);
        }

        @Override
        public void visit(MessageType messageType) {
            out = of(messageType);
        }

        @Override
        public void visit(PrimitiveType primitiveType) {
            out = of(primitiveType);
        }
    }
}
