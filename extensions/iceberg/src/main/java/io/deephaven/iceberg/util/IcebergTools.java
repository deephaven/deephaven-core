//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Tools for managing and manipulating tables on disk in parquet format.
 */
public class IcebergTools {

    @SuppressWarnings("unused")
    public static IcebergCatalog loadCatalog(final String name, final IcebergInstructions instructions) {
        return new IcebergCatalog(name, instructions);
    }

    static TableDefinition fromSchema(final Schema schema, PartitionSpec partitionSpec) {
        final Set<String> partitionNames =
                partitionSpec.fields().stream().map(PartitionField::name).collect(Collectors.toSet());

        final List<ColumnDefinition<?>> columns = new ArrayList<>();

        for (final Types.NestedField field : schema.columns()) {
            final String name = field.name();
            final Type type = field.type();
            final io.deephaven.qst.type.Type<?> qstType = convertPrimitiveType(type);
            final ColumnDefinition<?> column;
            if (partitionNames.contains(name)) {
                column = ColumnDefinition.of(name, qstType).withPartitioning();
            } else {
                column = ColumnDefinition.of(name, qstType);
            }
            columns.add(column);
        }

        return TableDefinition.of(columns);
    }

    static io.deephaven.qst.type.Type<?> convertPrimitiveType(final Type icebergType) {
        final Type.TypeID typeId = icebergType.typeId();
        if (icebergType.isPrimitiveType()) {
            if (typeId == Type.TypeID.BOOLEAN) {
                return io.deephaven.qst.type.Type.booleanType();
            } else if (typeId == Type.TypeID.DOUBLE) {
                return io.deephaven.qst.type.Type.doubleType();
            } else if (typeId == Type.TypeID.FLOAT) {
                return io.deephaven.qst.type.Type.floatType();
            } else if (typeId == Type.TypeID.INTEGER) {
                return io.deephaven.qst.type.Type.intType();
            } else if (typeId == Type.TypeID.LONG) {
                return io.deephaven.qst.type.Type.longType();
            } else if (typeId == Type.TypeID.STRING) {
                return io.deephaven.qst.type.Type.stringType();
            } else if (typeId == Type.TypeID.TIMESTAMP) {
                final Types.TimestampType timestampType = (Types.TimestampType) icebergType;
                return timestampType.shouldAdjustToUTC()
                        ? io.deephaven.qst.type.Type.find(Instant.class)
                        : io.deephaven.qst.type.Type.find(LocalDateTime.class);
            } else if (typeId == Type.TypeID.DATE) {
                return io.deephaven.qst.type.Type.find(java.time.LocalDate.class);
            } else if (typeId == Type.TypeID.TIME) {
                return io.deephaven.qst.type.Type.find(java.time.LocalTime.class);
            } else if (typeId == Type.TypeID.DECIMAL) {
                return io.deephaven.qst.type.Type.find(java.math.BigDecimal.class);
            } else if (typeId == Type.TypeID.FIXED || typeId == Type.TypeID.BINARY) {
                return io.deephaven.qst.type.Type.find(byte[].class);
            }
        }
        throw new TableDataException(
                "Unsupported iceberg column type " + typeId.name() +
                        " with logical type " + typeId.javaClass());
    }

    private IcebergTools() {}
}
