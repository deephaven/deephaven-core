//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.base;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.iceberg.util.IcebergWriteInstructions;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class IcebergUtils {

    /**
     * Get a stream of all {@link DataFile} objects from the given {@link Table} and {@link Snapshot}.
     *
     * @param table The {@link Table} to retrieve data files for.
     * @param snapshot The {@link Snapshot} to retrieve data files from.
     *
     * @return A stream of {@link DataFile} objects.
     */
    public static Stream<DataFile> allDataFiles(@NotNull final Table table, @NotNull final Snapshot snapshot) {
        try {
            return allManifests(table, snapshot).stream()
                    .map(manifestFile -> ManifestFiles.read(manifestFile, table.io()))
                    .flatMap(IcebergUtils::toStream);
        } catch (final RuntimeException e) {
            throw new TableDataException(
                    String.format("%s:%d - error retrieving manifest files", table, snapshot.snapshotId()), e);
        }
    }

    /**
     * Retrieves a {@link List} of manifest files from the given {@link Table} and {@link Snapshot}.
     *
     * @param table The {@link Table} to retrieve manifest files for.
     * @param snapshot The {@link Snapshot} to retrieve manifest files from.
     *
     * @return A {@link List} of {@link ManifestFile} objects.
     * @throws TableDataException if there is an error retrieving the manifest files.
     */
    static List<ManifestFile> allManifests(@NotNull final Table table, @NotNull final Snapshot snapshot) {
        try {
            return snapshot.allManifests(table.io());
        } catch (final RuntimeException e) {
            throw new TableDataException(
                    String.format("%s:%d - error retrieving manifest files", table, snapshot.snapshotId()), e);
        }
    }

    private static <T> Stream<T> toStream(final CloseableIterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).onClose(() -> {
            try {
                iterable.close();
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    /**
     * Convert an Iceberg data type to a Deephaven type.
     *
     * @param icebergType The Iceberg data type to be converted.
     * @return The converted Deephaven type.
     */
    public static io.deephaven.qst.type.Type<?> convertToDHType(@NotNull final Type icebergType) {
        final Type.TypeID typeId = icebergType.typeId();
        switch (typeId) {
            case BOOLEAN:
                return io.deephaven.qst.type.Type.booleanType().boxedType();
            case DOUBLE:
                return io.deephaven.qst.type.Type.doubleType();
            case FLOAT:
                return io.deephaven.qst.type.Type.floatType();
            case INTEGER:
                return io.deephaven.qst.type.Type.intType();
            case LONG:
                return io.deephaven.qst.type.Type.longType();
            case STRING:
                return io.deephaven.qst.type.Type.stringType();
            case TIMESTAMP:
                final Types.TimestampType timestampType = (Types.TimestampType) icebergType;
                return timestampType.shouldAdjustToUTC()
                        ? io.deephaven.qst.type.Type.find(Instant.class)
                        : io.deephaven.qst.type.Type.find(LocalDateTime.class);
            case DATE:
                return io.deephaven.qst.type.Type.find(LocalDate.class);
            case TIME:
                return io.deephaven.qst.type.Type.find(LocalTime.class);
            case DECIMAL:
                return io.deephaven.qst.type.Type.find(BigDecimal.class);
            case FIXED: // Fall through
            case BINARY:
                return io.deephaven.qst.type.Type.find(byte[].class);
            case UUID: // Fall through
            case STRUCT: // Fall through
            case LIST: // Fall through
            case MAP: // Fall through
            default:
                throw new TableDataException("Unsupported iceberg column type " + typeId.name());
        }
    }

    /**
     * Convert a Deephaven type to an Iceberg type.
     *
     * @param columnType The Deephaven type to be converted.
     * @return The converted Iceberg type.
     */
    public static Type convertToIcebergType(final Class<?> columnType) {
        if (columnType == Boolean.class) {
            return Types.BooleanType.get();
        } else if (columnType == double.class) {
            return Types.DoubleType.get();
        } else if (columnType == float.class) {
            return Types.FloatType.get();
        } else if (columnType == int.class) {
            return Types.IntegerType.get();
        } else if (columnType == long.class) {
            return Types.LongType.get();
        } else if (columnType == String.class) {
            return Types.StringType.get();
        } else if (columnType == Instant.class) {
            return Types.TimestampType.withZone();
        } else if (columnType == LocalDateTime.class) {
            return Types.TimestampType.withoutZone();
        } else if (columnType == LocalDate.class) {
            return Types.DateType.get();
        } else if (columnType == LocalTime.class) {
            return Types.TimeType.get();
        } else if (columnType == byte[].class) {
            return Types.BinaryType.get();
        } else {
            throw new TableDataException("Unsupported deephaven column type " + columnType.getName());
        }
        // TODO Add support for writing big decimals
        // TODO Add support for reading and writing lists
    }

    public static class SpecAndSchema {
        private final PartitionSpec partitionSpec;
        private final Schema schema;

        private SpecAndSchema(final PartitionSpec partitionSpec, final Schema schema) {
            this.partitionSpec = partitionSpec;
            this.schema = schema;
        }

        public PartitionSpec partitionSpec() {
            return partitionSpec;
        }

        public Schema schema() {
            return schema;
        }
    }

    /**
     * Create {@link PartitionSpec} and {@link Schema} from a {@link TableDefinition} using the provided instructions.
     */
    public static SpecAndSchema createSpecAndSchema(
            @NotNull final TableDefinition tableDefinition,
            @NotNull final IcebergWriteInstructions instructions) {
        final Collection<String> partitioningColumnNames = new ArrayList<>();
        final List<Types.NestedField> fields = new ArrayList<>();
        int fieldID = 1; // Iceberg field IDs start from 1

        // Create the schema first and use it to build the partition spec
        final Map<String, String> dhToIcebergColumnRenames = instructions.dhToIcebergColumnRenames();
        for (final ColumnDefinition<?> columnDefinition : tableDefinition.getColumns()) {
            final String dhColumnName = columnDefinition.getName();
            final String icebergColName = dhToIcebergColumnRenames.getOrDefault(dhColumnName, dhColumnName);
            final Type icebergType = convertToIcebergType(columnDefinition.getDataType());
            fields.add(Types.NestedField.optional(fieldID, icebergColName, icebergType));
            if (columnDefinition.isPartitioning()) {
                partitioningColumnNames.add(icebergColName);
            }
            fieldID++;
        }
        final Schema schema = new Schema(fields);

        final PartitionSpec partitionSpec = createPartitionSpec(schema, partitioningColumnNames);
        return new SpecAndSchema(partitionSpec, schema);
    }

    private static PartitionSpec createPartitionSpec(
            @NotNull final Schema schema,
            @NotNull final Iterable<String> partitionColumnNames) {
        final PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(schema);
        for (final String partitioningColumnName : partitionColumnNames) {
            partitionSpecBuilder.identity(partitioningColumnName);
        }
        return partitionSpecBuilder.build();
    }

    /**
     * Check if an existing iceberg table with provided schema is compatible for overwriting with a new table with given
     * schema.
     *
     * @param icebergSchema The schema of the existing iceberg table.
     * @param newSchema The schema of the new table.
     *
     * @throws IllegalArgumentException if the schemas are not compatible.
     */
    public static void verifyOverwriteCompatibility(
            final Schema icebergSchema,
            final Schema newSchema) {
        if (!icebergSchema.sameSchema(newSchema)) {
            throw new IllegalArgumentException("Schema mismatch, iceberg table schema: " + icebergSchema +
                    ", schema derived from the table definition: " + newSchema);
        }
    }

    /**
     * Check if an existing iceberg table with provided partition spec is compatible for overwriting with a new table
     * with given partition spec.
     *
     * @param icebergPartitionSpec The partition spec of the existing iceberg table.
     * @param newPartitionSpec The partition spec of the new table.
     *
     * @throws IllegalArgumentException if the partition spec are not compatible.
     */
    public static void verifyOverwriteCompatibility(
            final PartitionSpec icebergPartitionSpec,
            final PartitionSpec newPartitionSpec) {
        if (!icebergPartitionSpec.compatibleWith(newPartitionSpec)) {
            throw new IllegalArgumentException("Partition spec mismatch, iceberg table partition spec: " +
                    icebergPartitionSpec + ", partition spec derived from table definition: " + newPartitionSpec);
        }
    }

    /**
     * Check if an existing iceberg table with provided schema is compatible for appending deephaven table with provided
     * definition.
     *
     * @param icebergSchema The schema of the iceberg table.
     * @param tableDefinition The table definition of the deephaven table.
     *
     * @throws IllegalArgumentException if the schemas are not compatible.
     */
    public static void verifyAppendCompatibility(
            @NotNull final Schema icebergSchema,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final IcebergWriteInstructions instructions) {
        // Check that all columns in the table definition are part of the Iceberg schema and have the same type
        final Map<String, String> dhToIcebergColumnRenames = instructions.dhToIcebergColumnRenames();
        for (final ColumnDefinition<?> dhColumn : tableDefinition.getColumns()) {
            final String dhColumnName = dhColumn.getName();
            final String icebergColName = dhToIcebergColumnRenames.getOrDefault(dhColumnName, dhColumnName);
            final Types.NestedField icebergColumn = icebergSchema.findField(icebergColName);
            if (icebergColumn == null) {
                throw new IllegalArgumentException("Schema mismatch, column " + dhColumn.getName() + " from Deephaven "
                        + "table definition: " + tableDefinition + " is not found in Iceberg table schema: "
                        + icebergSchema);
            }
            if (!icebergColumn.type().equals(convertToIcebergType(dhColumn.getDataType()))) {
                throw new IllegalArgumentException("Schema mismatch, column " + dhColumn.getName() + " from Deephaven "
                        + "table definition: " + tableDefinition + " has type " + dhColumn.getDataType()
                        + " which does not match the type " + icebergColumn.type() + " in Iceberg table schema: "
                        + icebergSchema);
            }
        }

        // Check that all required columns in the Iceberg schema are part of the table definition
        final Map<String, String> icebergToDhColumnRenames = instructions.icebergToDhColumnRenames();
        for (final Types.NestedField icebergColumn : icebergSchema.columns()) {
            if (icebergColumn.isOptional()) {
                continue;
            }
            final String icebergColumnName = icebergColumn.name();
            final String dhColName = icebergToDhColumnRenames.getOrDefault(icebergColumnName, icebergColumnName);
            if (tableDefinition.getColumn(dhColName) == null) {
                throw new IllegalArgumentException("Schema mismatch, required column " + icebergColumnName
                        + " from Iceberg table schema: " + icebergSchema + " is not found in Deephaven table "
                        + "definition: " + tableDefinition);
            }
        }
    }

    /**
     * Check if an existing iceberg table with provided partition spec is compatible for appending deephaven table with
     * provided definition.
     *
     * @param partitionSpec The partition spec of the iceberg table.
     * @param tableDefinition The table definition of the deephaven table.
     *
     * @throws IllegalArgumentException if the partition spec are not compatible.
     */
    public static void verifyAppendCompatibility(
            @NotNull final PartitionSpec partitionSpec,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final IcebergWriteInstructions instructions) {
        final Map<String, String> icebergToDhColumnRenames = instructions.icebergToDhColumnRenames();
        final Set<String> icebergPartitionColumns = partitionSpec.fields().stream()
                .map(PartitionField::name)
                .map(icebergColumnName -> icebergToDhColumnRenames.getOrDefault(icebergColumnName, icebergColumnName))
                .collect(Collectors.toSet());
        final Set<String> dhPartitioningColumns = tableDefinition.getColumns().stream()
                .filter(ColumnDefinition::isPartitioning)
                .map(ColumnDefinition::getName)
                .collect(Collectors.toSet());
        if (!icebergPartitionColumns.equals(dhPartitioningColumns)) {
            throw new IllegalArgumentException("Partitioning column mismatch, iceberg table partition spec: " +
                    partitionSpec + ", deephaven table definition: " + tableDefinition);
        }
    }
}
