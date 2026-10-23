//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.base;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.iceberg.relative.RelativeFileIO;
import io.deephaven.iceberg.util.ColumnInstructions;
import io.deephaven.iceberg.util.Resolver;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
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

import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public final class IcebergUtils {

    public static String maybeResolveRelativePath(@NotNull final String path, @NotNull final FileIO io) {
        return io instanceof RelativeFileIO ? ((RelativeFileIO) io).absoluteLocation(path) : path;
    }

    public static URI locationUri(@NotNull final Table table) {
        return FileUtils.convertToURI(maybeResolveRelativePath(table.location(), table.io()), true);
    }

    public static URI dataFileUri(@NotNull final Table table, @NotNull final DataFile dataFile) {
        return FileUtils.convertToURI(maybeResolveRelativePath(dataFile.location(), table.io()), false);
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
            @NotNull final PartitionSpec tablePartitionSpec,
            @NotNull final TableDefinition tableDefinition) {
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

        for (final PartitionField partitionField : partitionFieldsFromSpec) {
            if (!partitioningColumnNamesFromDefinition.contains(partitionField.name())) {
                throw new IllegalArgumentException("Partitioning column " + partitionField.name() + " is not present " +
                        "in the table definition " + tableDefinition + ", partition spec " + tablePartitionSpec);
            }
        }
    }

    /**
     * Whether Deephaven places nulls where {@code nullOrder} does in a sort in {@code direction}. Deephaven orders null
     * before every value, so nulls come first in an ascending sort and last in a descending one.
     */
    public static boolean deephavenNullOrderMatchesIceberg(
            @NotNull final SortDirection direction,
            @NotNull final NullOrder nullOrder) {
        return nullOrder == (direction == SortDirection.ASC ? NullOrder.NULLS_FIRST : NullOrder.NULLS_LAST);
    }

    /**
     * Whether Deephaven, sorting a column of {@code dataType} in {@code direction}, orders the column's values and
     * nulls as the Iceberg specification orders a field of {@code icebergType} with {@code direction} and
     * {@code nullOrder}. Nulls must be placed as {@link #deephavenNullOrderMatchesIceberg(SortDirection, NullOrder)}
     * describes. Values are ordered alike for booleans, integers, dates, times and timestamps. Values are ordered
     * differently for strings, which Deephaven compares by UTF-16 code unit and the specification by code point, and
     * for floating point values, where Deephaven treats {@code -0.0} and {@code 0.0} as equal and orders every NaN
     * last, but the specification orders {@code -0.0} before {@code 0.0} and a negative NaN first.
     * <p>
     * A sort field's transform is not considered: the caller decides which transforms a sort by column values honors.
     */
    public static boolean deephavenOrderMatchesIceberg(
            @NotNull final SortDirection direction,
            @NotNull final NullOrder nullOrder,
            @NotNull final Class<?> dataType,
            @NotNull final Type icebergType) {
        if (!deephavenNullOrderMatchesIceberg(direction, nullOrder)) {
            return false;
        }
        switch (icebergType.typeId()) {
            case BOOLEAN:
                return dataType == Boolean.class;
            case INTEGER:
            case LONG:
                return dataType == byte.class || dataType == short.class || dataType == int.class
                        || dataType == long.class;
            case DATE:
                return dataType == LocalDate.class;
            case TIME:
                return dataType == LocalTime.class;
            case TIMESTAMP:
            case TIMESTAMP_NANO:
                return dataType == Instant.class || dataType == LocalDateTime.class;
            default:
                return false;
        }
    }
}
