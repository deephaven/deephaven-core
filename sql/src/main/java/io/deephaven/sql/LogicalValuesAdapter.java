//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import com.google.common.collect.ImmutableList;
import io.deephaven.qst.array.Array;
import io.deephaven.qst.array.ArrayBuilder;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.type.Type;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.TimestampString;

import java.time.Instant;
import java.util.function.Function;

final class LogicalValuesAdapter {

    public static TableSpec namedTable(LogicalValues values) {
        final NewTable.Builder builder = NewTable.builder().size(values.tuples.size());
        for (int fieldIx = 0; fieldIx < values.getRowType().getFieldCount(); ++fieldIx) {
            final RelDataTypeField field = values.getRowType().getFieldList().get(fieldIx);
            builder.addColumns(Column.of(field.getName(), array(values, fieldIx)));
        }
        return builder.build();
    }

    private static Array<?> array(LogicalValues values, final int fieldIx) {
        final RelDataTypeField field = values.getRowType().getFieldList().get(fieldIx);
        final RelDataType type = field.getValue();
        if (type.isStruct()) {
            throw new IllegalArgumentException();
        }
        switch (type.getSqlTypeName()) {
            case BOOLEAN:
                return fill(fieldIx, values, Boolean.class);
            case TINYINT:
                return fill(fieldIx, values, Byte.class);
            case SMALLINT:
                return fill(fieldIx, values, Short.class);
            case INTEGER:
                return fill(fieldIx, values, Integer.class);
            case BIGINT:
                return fill(fieldIx, values, Long.class);
            case REAL:
                return fill(fieldIx, values, Float.class);
            case DOUBLE:
            case FLOAT:
                return fill(fieldIx, values, Double.class);
            case TIMESTAMP:
                return fill(fieldIx, values, TimestampString.class, Instant.class, LogicalValuesAdapter::adapt);
            case CHAR:
                if (type.getPrecision() == 1) {
                    return fill(fieldIx, values, Character.class);
                }
                // SQLTODO(char-n-support)
                // Calcite treats VALUES constructs with "strings" as CHAR(N), meaning that strings w/ less than N get
                // padded with spaces. There may be ways to configure calcite to prefer VARCHAR over CHAR(N)?
                return fill(fieldIx, values, String.class);
            case VARCHAR:
                return fill(fieldIx, values, String.class);
        }
        throw new UnsupportedOperationException("Unsupported values type " + type.getSqlTypeName());
    }

    private static <T> Array<T> fill(final int fieldIx, LogicalValues values, Class<T> clazz) {
        return fill(fieldIx, values, clazz, clazz, Function.identity());
    }

    private static <T, R> Array<R> fill(final int fieldIx, LogicalValues values, Class<T> inClazz, Class<R> outClazz,
            Function<T, R> f) {
        final ArrayBuilder<R, ?, ?> builder = Array.builder(Type.find(outClazz), values.tuples.size());
        for (ImmutableList<RexLiteral> tuple : values.tuples) {
            builder.add(f.apply(tuple.get(fieldIx).getValueAs(inClazz)));
        }
        return builder.build();
    }

    private static Instant adapt(TimestampString ts) {
        return Instant.parse(ts.toString());
    }
}
