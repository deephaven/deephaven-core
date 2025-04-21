//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.base;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.iceberg.relative.RelativeFileIO;
import io.deephaven.iceberg.util.IcebergReadInstructions;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class IcebergUtils {

    private static final Map<Class<?>, Type> DH_TO_ICEBERG_TYPE_MAP = new HashMap<>();

    static {
        DH_TO_ICEBERG_TYPE_MAP.put(Boolean.class, Types.BooleanType.get());
        DH_TO_ICEBERG_TYPE_MAP.put(double.class, Types.DoubleType.get());
        DH_TO_ICEBERG_TYPE_MAP.put(float.class, Types.FloatType.get());
        DH_TO_ICEBERG_TYPE_MAP.put(int.class, Types.IntegerType.get());
        DH_TO_ICEBERG_TYPE_MAP.put(long.class, Types.LongType.get());
        DH_TO_ICEBERG_TYPE_MAP.put(String.class, Types.StringType.get());
        DH_TO_ICEBERG_TYPE_MAP.put(Instant.class, Types.TimestampType.withZone());
        DH_TO_ICEBERG_TYPE_MAP.put(LocalDateTime.class, Types.TimestampType.withoutZone());
        DH_TO_ICEBERG_TYPE_MAP.put(LocalDate.class, Types.DateType.get());
        DH_TO_ICEBERG_TYPE_MAP.put(LocalTime.class, Types.TimeType.get());
        // TODO (DH-18253): Add support for more types like ZonedDateTime, Big Decimals, and Lists
    }

    private static String path(@NotNull final String path, @NotNull final FileIO io) {
        return io instanceof RelativeFileIO ? ((RelativeFileIO) io).absoluteLocation(path) : path;
    }

    public static URI locationUri(@NotNull final Table table) {
        return FileUtils.convertToURI(path(table.location(), table.io()), true);
    }

    public static URI dataFileUri(@NotNull final Table table, @NotNull final DataFile dataFile) {
        return FileUtils.convertToURI(path(dataFile.path().toString(), table.io()), false);
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
                if (timestampType == Types.TimestampType.withZone()) {
                    return io.deephaven.qst.type.Type.find(Instant.class);
                }
                return io.deephaven.qst.type.Type.find(LocalDateTime.class);
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
        final Type icebergType = DH_TO_ICEBERG_TYPE_MAP.get(columnType);
        if (icebergType != null) {
            return icebergType;
        } else {
            throw new TableDataException("Unsupported deephaven column type " + columnType.getName());
        }
    }

    /**
     * Used to hold a {@link Schema}, {@link PartitionSpec} and {@link IcebergReadInstructions} together.
     */
    public static final class SpecAndSchema {
        public final Schema schema;
        public final PartitionSpec partitionSpec;
        public final IcebergReadInstructions readInstructions;

        public SpecAndSchema(
                @NotNull final Schema schema,
                @NotNull final PartitionSpec partitionSpec,
                @Nullable final IcebergReadInstructions readInstructions) {
            this.schema = schema;
            this.partitionSpec = partitionSpec;
            this.readInstructions = readInstructions;
        }
    }

    /**
     * Create {@link PartitionSpec} and {@link Schema} from a {@link TableDefinition}.
     *
     * @return A {@link SpecAndSchema} object containing the partition spec and schema, and {@code null} for read
     *         instructions.
     */
    public static SpecAndSchema createSpecAndSchema(@NotNull final TableDefinition tableDefinition) {
        final Collection<String> partitioningColumnNames = new ArrayList<>();
        final List<Types.NestedField> fields = new ArrayList<>();
        int fieldID = 1; // Iceberg field IDs start from 1

        // Create the schema first and use it to build the partition spec
        for (final ColumnDefinition<?> columnDefinition : tableDefinition.getColumns()) {
            final String dhColumnName = columnDefinition.getName();
            final Type icebergType = convertToIcebergType(columnDefinition.getDataType());
            fields.add(Types.NestedField.optional(fieldID, dhColumnName, icebergType));
            if (columnDefinition.isPartitioning()) {
                partitioningColumnNames.add(dhColumnName);
            }
            fieldID++;
        }
        final Schema schema = new Schema(fields);

        final PartitionSpec partitionSpec = createPartitionSpec(schema, partitioningColumnNames);
        return new SpecAndSchema(schema, partitionSpec, null);
    }

    public static PartitionSpec createPartitionSpec(
            @NotNull final Schema schema,
            @NotNull final Iterable<String> partitionColumnNames) {
        final PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(schema);
        for (final String partitioningColumnName : partitionColumnNames) {
            partitionSpecBuilder.identity(partitioningColumnName);
        }
        return partitionSpecBuilder.build();
    }

    public static boolean createNamespaceIfNotExists(
            @NotNull final Catalog catalog,
            @NotNull final Namespace namespace) {
        if (catalog instanceof SupportsNamespaces) {
            final SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
            try {
                nsCatalog.createNamespace(namespace);
                return true;
            } catch (final AlreadyExistsException | UnsupportedOperationException e) {
                return false;
            }
        }
        return false;
    }

    public static boolean dropNamespaceIfExists(
            @NotNull final Catalog catalog,
            @NotNull final Namespace namespace) {
        if (catalog instanceof SupportsNamespaces) {
            final SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
            try {
                return nsCatalog.dropNamespace(namespace);
            } catch (final NamespaceNotEmptyException e) {
                return false;
            }
        }
        return false;
    }

    /**
     * Check that all required fields are present in the table definition
     */
    public static void verifyRequiredFields(final Schema tableSchema, final TableDefinition tableDefinition) {
        final List<String> columnNames = tableDefinition.getColumnNames();
        for (final Types.NestedField field : tableSchema.columns()) {
            if (field.isRequired() && !columnNames.contains(field.name())) {
                // TODO (deephaven-core#6343): Add check for writeDefault() not set for required fields
                throw new IllegalArgumentException("Field " + field + " is required in the table schema, but is not " +
                        "present in the table definition, table schema " + tableSchema + ", tableDefinition " +
                        tableDefinition);
            }
        }
    }

    /**
     * Check that all the partitioning columns from the partition spec are present in the Table Definition.
     */
    public static void verifyPartitioningColumns(
            final PartitionSpec tablePartitionSpec,
            final TableDefinition tableDefinition) {
        final List<String> partitioningColumnNamesFromDefinition = tableDefinition.getColumnStream()
                .filter(ColumnDefinition::isPartitioning)
                .map(ColumnDefinition::getName)
                .collect(Collectors.toList());
        final List<PartitionField> partitionFieldsFromSchema = tablePartitionSpec.fields();
        if (partitionFieldsFromSchema.size() != partitioningColumnNamesFromDefinition.size()) {
            throw new IllegalArgumentException("Partition spec contains " + partitionFieldsFromSchema.size() +
                    " fields, but the table definition contains " + partitioningColumnNamesFromDefinition.size()
                    + " fields, partition spec " + tablePartitionSpec + ", table definition " + tableDefinition);
        }
        for (int colIdx = 0; colIdx < partitionFieldsFromSchema.size(); colIdx += 1) {
            final PartitionField partitionField = partitionFieldsFromSchema.get(colIdx);
            if (!partitioningColumnNamesFromDefinition.get(colIdx).equals(partitionField.name())) {
                throw new IllegalArgumentException("Partitioning column " + partitionField.name() + " is not present " +
                        "in the table definition at idx " + colIdx + ", table definition " + tableDefinition +
                        ", partition spec " + tablePartitionSpec);
            }
        }
    }
}
