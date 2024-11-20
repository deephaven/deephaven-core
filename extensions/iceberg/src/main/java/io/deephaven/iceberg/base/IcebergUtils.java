//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.base;

import io.deephaven.base.Pair;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
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
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.types.Conversions;
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
import java.util.Random;
import java.util.stream.Collectors;
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
     * Characters to be used for generating random variable names of length {@link #VARIABLE_NAME_LENGTH}.
     */
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static final int VARIABLE_NAME_LENGTH = 6;

    /**
     * Get a stream of all {@link DataFile} objects from the given {@link Table} and {@link Snapshot}.
     *
     * @param table The {@link Table} to retrieve data files for.
     * @param snapshot The {@link Snapshot} to retrieve data files from.
     *
     * @return A stream of {@link DataFile} objects.
     */
    public static Stream<DataFile> allDataFiles(@NotNull final Table table, @NotNull final Snapshot snapshot) {
        return allManifestFiles(table, snapshot)
                .map(manifestFile -> ManifestFiles.read(manifestFile, table.io()))
                .flatMap(IcebergUtils::toStream);
    }

    /**
     * Get a stream of all {@link ManifestFile} objects from the given {@link Table} and {@link Snapshot}.
     *
     * @param table The {@link Table} to retrieve manifest files for.
     * @param snapshot The {@link Snapshot} to retrieve manifest files from.
     *
     * @return A stream of {@link ManifestFile} objects.
     */
    public static Stream<ManifestFile> allManifestFiles(@NotNull final Table table, @NotNull final Snapshot snapshot) {
        return allManifests(table, snapshot).stream()
                .peek(manifestFile -> {
                    if (manifestFile.content() != ManifestContent.DATA) {
                        throw new TableDataException(
                                String.format(
                                        "%s:%d - only DATA manifest files are currently supported, encountered %s",
                                        table, snapshot.snapshotId(), manifestFile.content()));
                    }
                });
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

    /**
     * Convert a {@link org.apache.iceberg.io.CloseableIterable} to a {@link Stream} that will close the iterable when
     * the stream is closed.
     */
    public static <T> Stream<T> toStream(final org.apache.iceberg.io.CloseableIterable<T> iterable) {
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

    /**
     * Creates a list of {@link PartitionData} and corresponding update strings for Deephaven tables from partition
     * paths and spec. Also, validates that the partition paths are compatible with the provided partition spec.
     *
     * @param partitionSpec The partition spec to use for validation.
     * @param partitionPaths The list of partition paths to process.
     * @return A pair containing a list of PartitionData objects and a list of update strings for Deephaven tables.
     * @throws IllegalArgumentException if the partition paths are not compatible with the partition spec.
     */
    public static Pair<List<PartitionData>, List<String[]>> partitionDataFromPaths(
            final PartitionSpec partitionSpec,
            final Collection<String> partitionPaths) {
        final List<PartitionData> partitionDataList = new ArrayList<>(partitionPaths.size());
        final List<String[]> dhTableUpdateStringList = new ArrayList<>(partitionPaths.size());
        final int numPartitioningFields = partitionSpec.fields().size();
        final QueryScope queryScope = ExecutionContext.getContext().getQueryScope();
        for (final String partitionPath : partitionPaths) {
            final String[] dhTableUpdateString = new String[numPartitioningFields];
            try {
                final String[] partitions = partitionPath.split("/", -1);
                if (partitions.length != numPartitioningFields) {
                    throw new IllegalArgumentException("Expecting " + numPartitioningFields + " number of fields, " +
                            "found " + partitions.length);
                }
                final PartitionData partitionData = new PartitionData(partitionSpec.partitionType());
                for (int colIdx = 0; colIdx < partitions.length; colIdx += 1) {
                    final String[] parts = partitions[colIdx].split("=", 2);
                    if (parts.length != 2) {
                        throw new IllegalArgumentException("Expecting key=value format, found " + partitions[colIdx]);
                    }
                    final PartitionField field = partitionSpec.fields().get(colIdx);
                    if (!field.name().equals(parts[0])) {
                        throw new IllegalArgumentException("Expecting field name " + field.name() + " at idx " +
                                colIdx + ", found " + parts[0]);
                    }
                    final Type type = partitionData.getType(colIdx);
                    dhTableUpdateString[colIdx] = getTableUpdateString(field.name(), type, parts[1], queryScope);
                    partitionData.set(colIdx, Conversions.fromPartitionString(partitionData.getType(colIdx), parts[1]));
                }
            } catch (final Exception e) {
                throw new IllegalArgumentException("Failed to parse partition path: " + partitionPath + " using" +
                        " partition spec " + partitionSpec, e);
            }
            dhTableUpdateStringList.add(dhTableUpdateString);
            partitionDataList.add(DataFiles.data(partitionSpec, partitionPath));
        }
        return new Pair<>(partitionDataList, dhTableUpdateStringList);
    }

    /**
     * This method would convert a partitioning column info to a string which can be used in
     * {@link io.deephaven.engine.table.Table#updateView(Collection) Table#updateView} method. For example, if the
     * partitioning column of name "partitioningColumnName" if of type {@link Types.TimestampType} and the value is
     * "2021-01-01T00:00:00Z", then this method would:
     * <ul>
     * <li>Add a new parameter to the query scope with a random name and value as {@link Instant} parsed from the string
     * "2021-01-01T00:00:00Z"</li>
     * <li>Return the string "partitioningColumnName = randomName"</li>
     * </ul>
     *
     * @param colName The name of the partitioning column
     * @param colType The type of the partitioning column
     * @param value The value of the partitioning column
     * @param queryScope The query scope to add the parameter to
     */
    private static String getTableUpdateString(
            @NotNull final String colName,
            @NotNull final Type colType,
            @NotNull final String value,
            @NotNull final QueryScope queryScope) {
        // Randomly generated name to be added to the query scope for each value to avoid repeated casts
        // TODO Is this the right approach? Also, how would we clean up these params?
        final String paramName = generateRandomAlphabetString(VARIABLE_NAME_LENGTH);
        final Type.TypeID typeId = colType.typeId();
        if (typeId == Type.TypeID.BOOLEAN) {
            queryScope.putParam(paramName, Boolean.parseBoolean(value));
        } else if (typeId == Type.TypeID.DOUBLE) {
            queryScope.putParam(paramName, Double.parseDouble(value));
        } else if (typeId == Type.TypeID.FLOAT) {
            queryScope.putParam(paramName, Float.parseFloat(value));
        } else if (typeId == Type.TypeID.INTEGER) {
            queryScope.putParam(paramName, Integer.parseInt(value));
        } else if (typeId == Type.TypeID.LONG) {
            queryScope.putParam(paramName, Long.parseLong(value));
        } else if (typeId == Type.TypeID.STRING) {
            queryScope.putParam(paramName, value);
        } else if (typeId == Type.TypeID.TIMESTAMP) {
            final Types.TimestampType timestampType = (Types.TimestampType) colType;
            if (timestampType == Types.TimestampType.withZone()) {
                queryScope.putParam(paramName, Instant.parse(value));
            } else {
                queryScope.putParam(paramName, LocalDateTime.parse(value));
            }
        } else if (typeId == Type.TypeID.DATE) {
            queryScope.putParam(paramName, LocalDate.parse(value));
        } else if (typeId == Type.TypeID.TIME) {
            queryScope.putParam(paramName, LocalTime.parse(value));
        } else {
            // TODO (deephaven-core#6327) Add support for more types like ZonedDateTime, Big Decimals
            throw new TableDataException("Unsupported partitioning column type " + typeId.name());
        }
        return colName + " = " + paramName;
    }

    /**
     * Generate a random string of length {@code length} using just alphabets.
     */
    private static String generateRandomAlphabetString(final int length) {
        final StringBuilder stringBuilder = new StringBuilder();
        final Random random = new Random();
        for (int i = 0; i < length; i++) {
            final int index = random.nextInt(CHARACTERS.length());
            stringBuilder.append(CHARACTERS.charAt(index));
        }
        return stringBuilder.toString();
    }
}
