//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.base;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.iceberg.relative.RelativeFileIO;
import io.deephaven.iceberg.util.TypeInference;
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
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        DH_TO_ICEBERG_TYPE_MAP.put(byte[].class, Types.BinaryType.get());
        // TODO (deephaven-core#6327) Add support for more types like ZonedDateTime, Big Decimals, and Lists
    }

    public static String maybeResolveRelativePath(@NotNull final String path, @NotNull final FileIO io) {
        return io instanceof RelativeFileIO ? ((RelativeFileIO) io).absoluteLocation(path) : path;
    }

    public static URI locationUri(@NotNull final Table table) {
        return FileUtils.convertToURI(maybeResolveRelativePath(table.location(), table.io()), true);
    }

    public static URI dataFileUri(@NotNull final Table table, @NotNull final DataFile dataFile) {
        return FileUtils.convertToURI(maybeResolveRelativePath(dataFile.location(), table.io()), false);
    }

    /**
     * Convert an Iceberg data type to a Deephaven type.
     *
     * @param icebergType The Iceberg data type to be converted.
     * @return The converted Deephaven type.
     * @deprecated prefer {@link TypeInference#of(Type)}
     */
    // TODO(DH-19288): Remove deprecated items after Iceberg update
    @Deprecated(forRemoval = true)
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
     * @deprecated prefer {@link TypeInference#of(io.deephaven.qst.type.Type, TypeUtil.NextID)}
     */
    // TODO(DH-19288): Remove deprecated items after Iceberg update
    @Deprecated(forRemoval = true)
    public static Type convertToIcebergType(final Class<?> columnType) {
        final Type icebergType = DH_TO_ICEBERG_TYPE_MAP.get(columnType);
        if (icebergType != null) {
            return icebergType;
        } else {
            throw new TableDataException("Unsupported deephaven column type " + columnType.getName());
        }
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

    private static final Set<Class<?>> SUPPORTED_PARTITIONING_TYPES =
            Set.of(Boolean.class, double.class, float.class, int.class, long.class, String.class, LocalDate.class);

    /**
     * Check that all the partitioning columns from the partition spec are of supported types and are present in the
     * Table Definition.
     */
    public static void verifyPartitioningColumns(
            final PartitionSpec tablePartitionSpec,
            final TableDefinition tableDefinition) {
        final List<String> partitioningColumnNamesFromDefinition = tableDefinition.getColumnStream()
                .filter(ColumnDefinition::isPartitioning)
                .peek(columnDefinition -> {
                    if (!SUPPORTED_PARTITIONING_TYPES.contains(columnDefinition.getDataType())) {
                        throw new IllegalArgumentException("Unsupported partitioning column type " +
                                columnDefinition.getDataType() + " for column " + columnDefinition.getName());
                    }
                })
                .map(ColumnDefinition::getName)
                .collect(Collectors.toList());
        final List<PartitionField> partitionFieldsFromSpec = tablePartitionSpec.fields();
        partitionFieldsFromSpec.forEach(partitionField -> {
            if (!partitionField.transform().isIdentity()) {
                // TODO (DH-18160): Improve support for handling non-identity transforms
                throw new IllegalArgumentException("Partitioning column " + partitionField.name() +
                        " has non-identity transform of type `" + partitionField.transform() + "`, currently we do" +
                        " not support writing to iceberg tables with non-identity transforms");
            }
        });
        if (partitionFieldsFromSpec.size() != partitioningColumnNamesFromDefinition.size()) {
            throw new IllegalArgumentException("Partition spec contains " + partitionFieldsFromSpec.size() +
                    " fields, but the table definition contains " + partitioningColumnNamesFromDefinition.size()
                    + " fields, partition spec " + tablePartitionSpec + ", table definition " + tableDefinition);
        }
        for (int colIdx = 0; colIdx < partitionFieldsFromSpec.size(); colIdx += 1) {
            final PartitionField partitionField = partitionFieldsFromSpec.get(colIdx);
            if (!partitioningColumnNamesFromDefinition.get(colIdx).equals(partitionField.name())) {
                throw new IllegalArgumentException("Partitioning column " + partitionField.name() + " is not present " +
                        "in the table definition at idx " + colIdx + ", table definition " + tableDefinition +
                        ", partition spec " + tablePartitionSpec);
            }
        }
    }
}
