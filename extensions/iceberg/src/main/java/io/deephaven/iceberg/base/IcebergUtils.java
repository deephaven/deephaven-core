//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.base;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.iceberg.relative.RelativeFileIO;
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
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.time.LocalDate;
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
