//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.base;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.iceberg.util.IcebergReadInstructions;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

    /**
     * Get a stream of all {@link DataFile} objects from the given {@link Table} and {@link Snapshot}.
     *
     * @param table The {@link Table} to retrieve data files for.
     * @param snapshot The {@link Snapshot} to retrieve data files from.
     *
     * @return A stream of {@link DataFile} objects.
     */
    public static Stream<DataFile> allDataFiles(@NotNull final Table table, @NotNull final Snapshot snapshot) {
        final List<ManifestFile> manifestFiles = allManifests(table, snapshot);
        // Sort manifest files by sequence number to read data files in the correct commit order
        manifestFiles.sort(Comparator.comparingLong(ManifestFile::sequenceNumber));
        return manifestFiles.stream()
                .peek(manifestFile -> {
                    if (manifestFile.content() != ManifestContent.DATA) {
                        throw new TableDataException(
                                String.format(
                                        "%s:%d - only DATA manifest files are currently supported, encountered %s",
                                        table, snapshot.snapshotId(), manifestFile.content()));
                    }
                })
                .map(manifestFile -> ManifestFiles.read(manifestFile, table.io()))
                .flatMap(IcebergUtils::toStream);
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
     * Verifies that the new schema is compatible with the existing schema for writing.
     */
    public static void verifyWriteCompatibility(final Schema existingSchema, final Schema newSchema) {
        // Check that all fields in the new schema are present in the existing schema
        for (final Types.NestedField newField : newSchema.columns()) {
            final Types.NestedField existingSchemaField = existingSchema.findField(newField.fieldId());

            if (existingSchemaField == null) {
                throw new IllegalArgumentException(
                        "Field " + newField.name() + " is not present in the existing schema");
            }

            if (!existingSchemaField.equals(newField)) {
                throw new IllegalArgumentException("Field " + newField + " is not identical with the field " +
                        existingSchemaField + " from existing schema");
            }
        }

        // Check that all required fields are present in the new schema
        for (final Types.NestedField existingField : existingSchema.columns()) {
            if (existingField.isRequired() && newSchema.findField(existingField.fieldId()) == null) {
                // TODO (deephaven-core#6343): Add check for writeDefault() not set for required fields
                throw new IllegalArgumentException("Field " + existingField + " is required in the existing table " +
                        "schema, but is not present in the new schema");
            }
        }
    }

    /**
     * Creates a list of {@link PartitionData} objects from a list of partition paths using the provided partition spec.
     * Also, validates internally that the partition paths are compatible with the partition spec.
     *
     * @param partitionSpec The partition spec to use for validation
     * @param partitionPaths The list of partition paths to create PartitionData objects from
     *
     * @return A list of PartitionData objects
     */
    public static List<PartitionData> partitionDataFromPaths(
            final PartitionSpec partitionSpec,
            final Collection<String> partitionPaths) {
        final List<PartitionData> partitionDataList = new ArrayList<>(partitionPaths.size());
        for (final String partitionPath : partitionPaths) {
            // Following will internally validate the structure and values of the partition path
            try {
                partitionDataList.add(DataFiles.data(partitionSpec, partitionPath));
            } catch (final Exception e) {
                throw new IllegalArgumentException("Failed to parse partition path: " + partitionPath + " using" +
                        " partition spec " + partitionSpec, e);
            }
        }
        return partitionDataList;
    }
}
