//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.base.Pair;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.context.StandaloneQueryScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.iceberg.base.IcebergUtils;
import io.deephaven.iceberg.internal.DataInstructionsProviderLoader;
import io.deephaven.parquet.table.CompletedParquetWrite;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.iceberg.util.SchemaProviderInternal.SchemaProviderImpl;
import io.deephaven.util.SafeCloseable;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import static io.deephaven.iceberg.base.IcebergUtils.convertToIcebergType;
import static io.deephaven.iceberg.base.IcebergUtils.verifyPartitioningColumns;
import static io.deephaven.iceberg.base.IcebergUtils.verifyRequiredFields;

/**
 * This class is responsible for writing Deephaven tables to an Iceberg table. Each instance of this class is associated
 * with a single {@link IcebergTableAdapter} and can be used to write multiple Deephaven tables to this Iceberg table.
 */
public class IcebergTableWriter {

    /**
     * The options used to configure the behavior of this writer instance.
     */
    private final TableParquetWriterOptions tableWriterOptions;

    /**
     * The Iceberg table which will be written to by this instance.
     */
    private final org.apache.iceberg.Table table;

    /**
     * Store the partition spec of the Iceberg table at the time of creation of this writer instance and use it for all
     * writes, so that even if the table spec changes, the writer will still work.
     */
    private final PartitionSpec tableSpec;

    /**
     * The table definition used for all writes by this writer instance.
     */
    private final TableDefinition tableDefinition;

    /**
     * The table definition consisting of non-partitioning columns from {@link #tableDefinition}. All tables written by
     * this writer are expected to have a compatible definition with this.
     */
    private final TableDefinition nonPartitioningTableDefinition;

    /**
     * The schema to use when in conjunction with the {@link #fieldIdToColumnName} to map Deephaven columns from
     * {@link #tableDefinition} to Iceberg columns.
     */
    private final Schema userSchema;

    /**
     * Mapping from Iceberg field IDs to Deephaven column names, populated inside the parquet file.
     * <p>
     * Use this map instead of the {@link TableWriterOptions#fieldIdToColumnName()} map after initialization to ensure
     * that all columns in the table definition are accounted for.
     */
    private final Map<Integer, String> fieldIdToColumnName;

    /**
     * The factory to create new output file locations for writing data files.
     */
    private final OutputFileFactory outputFileFactory;

    /**
     * The sort order to write down for new data files.
     */
    private final SortOrder sortOrderToWrite;

    /**
     * The names of columns on which the tables will be sorted before writing to Iceberg.
     */
    private final Collection<SortColumn> sortColumnNames;

    /**
     * The special instructions to use for writing the Iceberg data files (might be S3Instructions or other cloud
     * provider-specific instructions).
     */
    private final Object specialInstructions;

    /**
     * Characters to be used for generating random variable names of length {@link #VARIABLE_NAME_LENGTH}.
     */
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static final int VARIABLE_NAME_LENGTH = 6;

    /**
     * Create a new Iceberg table writer instance.
     *
     * @param tableWriterOptions The options to configure the behavior of this writer instance.
     * @param tableAdapter The Iceberg table adapter corresponding to the Iceberg table to write to.
     * @param dataInstructionsProvider The provider for special instructions, to be used if special instructions not
     *        provided in the {@code tableWriterOptions}.
     */
    IcebergTableWriter(
            final TableWriterOptions tableWriterOptions,
            final IcebergTableAdapter tableAdapter,
            final DataInstructionsProviderLoader dataInstructionsProvider) {
        this.tableWriterOptions = verifyWriterOptions(tableWriterOptions);
        this.table = tableAdapter.icebergTable();

        this.tableSpec = table.spec();

        this.tableDefinition = tableWriterOptions.tableDefinition();
        this.nonPartitioningTableDefinition = nonPartitioningTableDefinition(tableDefinition);
        verifyRequiredFields(table.schema(), tableDefinition);
        verifyPartitioningColumns(tableSpec, tableDefinition);

        this.userSchema = ((SchemaProviderImpl) tableWriterOptions.schemaProvider()).getSchema(table);
        verifyFieldIdsInSchema(tableWriterOptions.fieldIdToColumnName().keySet(), userSchema);

        // Create a copy of the fieldIdToColumnName map since we might need to add new entries for columns which are not
        // provided by the user.
        this.fieldIdToColumnName = new HashMap<>(tableWriterOptions.fieldIdToColumnName());
        addFieldIdsForAllColumns();

        outputFileFactory = OutputFileFactory.builderFor(table, 0, 0)
                .format(FileFormat.PARQUET)
                .build();

        final SortOrderProviderInternal.SortOrderProviderImpl sortOrderProvider =
                ((SortOrderProviderInternal.SortOrderProviderImpl) tableWriterOptions.sortOrderProvider());
        sortColumnNames =
                computeSortColumns(sortOrderProvider.getSortOrderToUse(table), sortOrderProvider.failOnUnmapped());
        sortOrderToWrite = sortOrderProvider.getSortOrderToWrite(table);

        final String uriScheme = tableAdapter.locationUri().getScheme();
        this.specialInstructions = tableWriterOptions.dataInstructions()
                .orElseGet(() -> dataInstructionsProvider.load(uriScheme));

    }

    private static TableParquetWriterOptions verifyWriterOptions(
            @NotNull final TableWriterOptions tableWriterOptions) {
        // We ony support writing to Parquet files
        if (!(tableWriterOptions instanceof TableParquetWriterOptions)) {
            throw new IllegalArgumentException(
                    "Unsupported options of class " + tableWriterOptions.getClass() + " for" +
                            " writing Iceberg table, expected: " + TableParquetWriterOptions.class);
        }
        return (TableParquetWriterOptions) tableWriterOptions;
    }

    /**
     * Return a {@link TableDefinition} which contains only the non-partitioning columns from the provided table
     * definition.
     */
    private static TableDefinition nonPartitioningTableDefinition(
            @NotNull final TableDefinition tableDefinition) {
        final Collection<ColumnDefinition<?>> nonPartitioningColumns = new ArrayList<>();
        for (final ColumnDefinition<?> columnDefinition : tableDefinition.getColumns()) {
            if (!columnDefinition.isPartitioning()) {
                nonPartitioningColumns.add(columnDefinition);
            }
        }
        return TableDefinition.of(nonPartitioningColumns);
    }

    /**
     * Check that all the field IDs are present in the schema.
     */
    private static void verifyFieldIdsInSchema(final Collection<Integer> fieldIds, final Schema schema) {
        if (!fieldIds.isEmpty()) {
            for (final Integer fieldId : fieldIds) {
                if (schema.findField(fieldId) == null) {
                    throw new IllegalArgumentException("Column corresponding to field ID " + fieldId + " not " +
                            "found in schema, available columns in schema are: " + schema.columns());
                }
            }
        }
    }

    /**
     * Populate the {@link #fieldIdToColumnName} map for all the columns in the {@link #tableDefinition} and do
     * additional checks to ensure that the table definition is compatible with schema provided by user.
     */
    private void addFieldIdsForAllColumns() {
        final Map<String, Integer> dhColumnNameToFieldId = tableWriterOptions.dhColumnNameToFieldId();
        Map<String, Integer> nameMappingDefault = null; // Lazily initialized
        for (final ColumnDefinition<?> columnDefinition : tableDefinition.getColumns()) {
            final String columnName = columnDefinition.getName();

            // We are done if we already have the mapping between column name and field ID
            if (dhColumnNameToFieldId.containsKey(columnName)) {
                continue;
            }

            // To be populated by the end of this block for each column, else throw an exception
            Integer fieldId = null;
            Types.NestedField nestedField = null;

            // Check in the schema.name_mapping.default map
            if (nameMappingDefault == null) {
                nameMappingDefault = readNameMappingDefault();
            }
            fieldId = nameMappingDefault.get(columnName);
            if (fieldId != null) {
                nestedField = userSchema.findField(fieldId);
                if (nestedField == null) {
                    throw new IllegalArgumentException("Field ID " + fieldId + " extracted for " +
                            "column " + columnName + " from the schema.name_mapping map not found in schema " +
                            userSchema);
                }
            }

            // Directly lookup in the user provided schema using column name
            if (fieldId == null) {
                nestedField = userSchema.findField(columnName);
                if (nestedField != null) {
                    fieldId = nestedField.fieldId();
                }
            }

            if (nestedField == null) {
                throw new IllegalArgumentException("Column " + columnName + " not found in the schema or " +
                        "the name mapping for the table");
            }
            final Type expectedIcebergType = nestedField.type();
            final Class<?> dhType = columnDefinition.getDataType();
            final Type convertedIcebergType = convertToIcebergType(dhType);
            if (!expectedIcebergType.equals(convertedIcebergType)) {
                throw new IllegalArgumentException("Column " + columnName + " has type " + dhType + " in table " +
                        "definition but type " + expectedIcebergType + " in Iceberg schema");
            }

            fieldIdToColumnName.put(fieldId, columnName);
        }
    }

    /**
     * Build the mapping from column names to field IDs on demand using the
     * {@value TableProperties#DEFAULT_NAME_MAPPING} map.
     * <p>
     * Return an empty map if the table metadata is null or the mapping is not present in the table metadata.
     */
    private Map<String, Integer> readNameMappingDefault() {
        final TableMetadata tableMetadata;
        if (table instanceof HasTableOperations) {
            tableMetadata = ((HasTableOperations) table).operations().current();
        } else {
            // TableMetadata is not available, so nothing to add to the map
            return Map.of();
        }
        final String nameMappingJson = tableMetadata.property(TableProperties.DEFAULT_NAME_MAPPING, null);
        if (nameMappingJson == null) {
            return Map.of();
        }
        // Iterate over all mapped fields and build a reverse map from column name to field ID
        final Map<String, Integer> nameMappingDefault = new HashMap<>();
        final NameMapping nameMapping = NameMappingParser.fromJson(nameMappingJson);
        for (final MappedField field : nameMapping.asMappedFields().fields()) {
            final Integer fieldId = field.id();
            for (final String name : field.names()) {
                nameMappingDefault.put(name, fieldId);
            }
        }
        return nameMappingDefault;
    }

    private List<SortColumn> computeSortColumns(@NotNull final SortOrder sortOrder, final boolean failOnUnmapped) {
        if (sortOrder.isUnsorted()) {
            return List.of();
        }
        final List<SortField> sortFields = sortOrder.fields();
        final List<SortColumn> sortColumns = new ArrayList<>(sortFields.size());
        for (final SortField sortField : sortOrder.fields()) {
            final boolean ascending;
            if (sortField.nullOrder() == NullOrder.NULLS_FIRST && sortField.direction() == SortDirection.ASC) {
                ascending = true;
            } else if (sortField.nullOrder() == NullOrder.NULLS_LAST && sortField.direction() == SortDirection.DESC) {
                ascending = false;
            } else {
                if (failOnUnmapped) {
                    throw new IllegalArgumentException(
                            "Cannot apply sort order " + sortOrder + " since Deephaven currently only supports " +
                                    "sorting by {ASC, NULLS FIRST} or {DESC, NULLS LAST}");
                }
                return List.of();
            }
            final int fieldId = sortField.sourceId();
            final String columnName = fieldIdToColumnName.get(fieldId);
            if (columnName == null) {
                if (failOnUnmapped) {
                    throw new IllegalArgumentException("Cannot apply sort order " + sortOrder + " since column " +
                            "corresponding to field ID " + fieldId + " not found in schema");
                }
                return List.of();
            }
            final SortColumn sortColumn =
                    ascending ? SortColumn.asc(ColumnName.of(columnName)) : SortColumn.desc(ColumnName.of(columnName));
            sortColumns.add(sortColumn);
        }
        return sortColumns;
    }

    /**
     * Append the provided Deephaven {@link IcebergWriteInstructions#tables()} as new partitions to the existing Iceberg
     * table in a single snapshot. This method will not perform any compatibility checks between the existing schema and
     * the provided Deephaven tables.
     *
     * @param writeInstructions The instructions for customizations while writing.
     */
    public void append(@NotNull final IcebergWriteInstructions writeInstructions) {
        final List<DataFile> dataFilesWritten = writeDataFiles(writeInstructions);
        commit(dataFilesWritten);
    }

    /**
     * Writes data from Deephaven {@link IcebergWriteInstructions#tables()} to an Iceberg table without creating a new
     * snapshot. This method returns a list of data files that were written. Users can use this list to create a
     * transaction/snapshot if needed. This method will not perform any compatibility checks between the existing schema
     * and the provided Deephaven tables.
     *
     * @param writeInstructions The instructions for customizations while writing.
     */
    public List<DataFile> writeDataFiles(@NotNull final IcebergWriteInstructions writeInstructions) {
        verifyCompatible(writeInstructions.tables(), nonPartitioningTableDefinition);
        final List<String> partitionPaths = writeInstructions.partitionPaths();
        verifyPartitionPaths(tableSpec, partitionPaths);
        final List<PartitionData> partitionData;
        final List<CompletedParquetWrite> parquetFileInfo;
        // Start a new query scope to avoid polluting the existing query scope with new parameters added for
        // partitioning columns
        try (final SafeCloseable _ignore =
                ExecutionContext.getContext().withQueryScope(new StandaloneQueryScope()).open()) {
            final Pair<List<PartitionData>, List<String[]>> ret = partitionDataFromPaths(tableSpec, partitionPaths);
            partitionData = ret.getFirst();
            final List<String[]> dhTableUpdateStrings = ret.getSecond();
            parquetFileInfo = writeTables(partitionData, dhTableUpdateStrings, writeInstructions);
        }
        return dataFilesFromParquet(parquetFileInfo, partitionData);
    }

    /**
     * Verify that all the tables are compatible with the provided table definition.
     */
    private static void verifyCompatible(
            @NotNull final Iterable<Table> tables,
            @NotNull final TableDefinition expectedDefinition) {
        for (final Table table : tables) {
            try {
                expectedDefinition.checkMutualCompatibility(table.getDefinition());
            } catch (final Exception e) {
                throw new TableDefinition.IncompatibleTableDefinitionException("Actual table definition is not " +
                        "compatible with the expected definition, actual = " + table.getDefinition() + ", expected = "
                        + expectedDefinition, e);
            }
        }
    }

    private static void verifyPartitionPaths(
            final PartitionSpec partitionSpec,
            final Collection<String> partitionPaths) {
        if (partitionSpec.isPartitioned() && partitionPaths.isEmpty()) {
            throw new IllegalArgumentException("Cannot write data to a partitioned table without partition paths.");
        }
        if (!partitionSpec.isPartitioned() && !partitionPaths.isEmpty()) {
            throw new IllegalArgumentException("Cannot write data to an un-partitioned table with partition paths.");
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
     *
     * @implNote Check implementations of {@link DataFiles#data} and {@link Conversions#fromPartitionString} for more
     *           details on how partition paths should be parsed, how each type of value is parsed from a string and
     *           what types are allowed for partitioning columns.
     */
    private static Pair<List<PartitionData>, List<String[]>> partitionDataFromPaths(
            final PartitionSpec partitionSpec,
            final Collection<String> partitionPaths) {
        final List<PartitionData> partitionDataList = new ArrayList<>(partitionPaths.size());
        final List<String[]> dhTableUpdateStringList = new ArrayList<>(partitionPaths.size());
        final int numPartitioningFields = partitionSpec.fields().size();
        final QueryScope queryScope = ExecutionContext.getContext().getQueryScope();
        for (final String partitionPath : partitionPaths) {
            final String[] dhTableUpdateString = new String[numPartitioningFields];
            final PartitionData partitionData = new PartitionData(partitionSpec.partitionType());
            try {
                final String[] partitions = partitionPath.split("/", -1);
                if (partitions.length != numPartitioningFields) {
                    throw new IllegalArgumentException("Expecting " + numPartitioningFields + " number of fields, " +
                            "found " + partitions.length);
                }

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
                    partitionData.set(colIdx, Conversions.fromPartitionString(type, parts[1]));
                }
            } catch (final Exception e) {
                throw new IllegalArgumentException("Failed to parse partition path: " + partitionPath + " using" +
                        " partition spec " + partitionSpec + ", check cause for more details ", e);
            }
            dhTableUpdateStringList.add(dhTableUpdateString);
            partitionDataList.add(partitionData);
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
        // TODO(deephaven-core#6418): Find a better way to handle these table updates instead of using query scope
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
        } else if (typeId == Type.TypeID.DATE) {
            queryScope.putParam(paramName, LocalDate.parse(value));
        } else {
            // TODO (deephaven-core#6327) Add support for more partitioning types like Big Decimals
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

    /**
     * Write the provided Deephaven tables to parquet files and return a list of {@link CompletedParquetWrite} objects
     * for each table written.
     *
     * @param partitionDataList The list of {@link PartitionData} objects for each table, empty if the table is not
     *        partitioned.
     * @param dhTableUpdateStrings The list of update strings to be applied using {@link Table#updateView}, empty if the
     *        table is not partitioned.
     * @param writeInstructions The instructions for customizations while writing.
     */
    @NotNull
    private List<CompletedParquetWrite> writeTables(
            @NotNull final List<PartitionData> partitionDataList,
            @NotNull final List<String[]> dhTableUpdateStrings,
            @NotNull final IcebergWriteInstructions writeInstructions) {
        final List<Table> dhTables = writeInstructions.tables();
        final boolean isPartitioned = tableSpec.isPartitioned();
        if (isPartitioned) {
            Require.eq(dhTables.size(), "dhTables.size()",
                    partitionDataList.size(), "partitionDataList.size()");
            Require.eq(dhTables.size(), "dhTables.size()",
                    dhTableUpdateStrings.size(), "dhTableUpdateStrings.size()");
        } else {
            Require.eqZero(partitionDataList.size(), "partitionDataList.size()");
            Require.eqZero(dhTableUpdateStrings.size(), "dhTableUpdateStrings.size()");
        }

        // Build the parquet instructions
        final List<CompletedParquetWrite> parquetFilesWritten = new ArrayList<>(dhTables.size());
        final ParquetInstructions.OnWriteCompleted onWriteCompleted = parquetFilesWritten::add;
        final ParquetInstructions parquetInstructions = tableWriterOptions.toParquetInstructions(
                onWriteCompleted, tableDefinition, fieldIdToColumnName, specialInstructions);

        // Write the data to parquet files
        final int numTables = dhTables.size();
        for (int idx = 0; idx < numTables; idx++) {
            final Table dhTable = dhTables.get(idx);
            final PartitionData partitionData;
            final String[] dhTableUpdateString;
            if (isPartitioned) {
                partitionData = partitionDataList.get(idx);
                dhTableUpdateString = dhTableUpdateStrings.get(idx);
            } else {
                partitionData = null;
                dhTableUpdateString = null;
            }
            writeTable(dhTable, isPartitioned, partitionData, dhTableUpdateString, parquetInstructions);
        }
        return parquetFilesWritten;
    }

    /**
     * Write the provided Deephaven table to a parquet file.
     *
     * @param dhTable The Deephaven table to write.
     * @param isPartitioned Whether the iceberg table is partitioned.
     * @param partitionData The partition data for the table, null if the iceberg table is not partitioned.
     * @param dhTableUpdateString The update string to apply to the table, null if the iceberg table is not partitioned.
     * @param parquetInstructions The instructions for customizations while writing parquet.
     */
    private void writeTable(
            @NotNull final Table dhTable,
            final boolean isPartitioned,
            @Nullable final PartitionData partitionData,
            @Nullable final String[] dhTableUpdateString,
            @NotNull final ParquetInstructions parquetInstructions) {
        if (dhTable.numColumns() == 0) {
            // Skip writing empty tables with no columns
            return;
        }
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final String newDataLocation;
            Table dhTableToWrite = dhTable;
            if (isPartitioned) {
                newDataLocation = IcebergUtils
                        .maybeResolveRelativePath(getDataLocation(Objects.requireNonNull(partitionData)), table.io());
                dhTableToWrite = dhTableToWrite.updateView(Objects.requireNonNull(dhTableUpdateString));
            } else {
                newDataLocation = getDataLocation();
            }

            if (!sortColumnNames.isEmpty()) {
                dhTableToWrite = dhTableToWrite.sort(sortColumnNames);
            }

            // TODO (deephaven-core#6343): Set writeDefault() values for required columns that are not present in table
            ParquetTools.writeTable(dhTableToWrite, newDataLocation, parquetInstructions);
        }
    }

    /**
     * Generate the location string for a new data file for the given partition data.
     */
    private String getDataLocation(@NotNull final PartitionData partitionData) {
        final EncryptedOutputFile outputFile = outputFileFactory.newOutputFile(tableSpec, partitionData);
        return outputFile.encryptingOutputFile().location();
    }

    /**
     * Generate the location string for a new data file for the unpartitioned table.
     */
    private String getDataLocation() {
        final EncryptedOutputFile outputFile = outputFileFactory.newOutputFile();
        return outputFile.encryptingOutputFile().location();
    }

    /**
     * Commit the changes to the Iceberg table by creating a snapshot.
     */
    private void commit(@NotNull final Iterable<DataFile> dataFiles) {
        final Transaction icebergTransaction = table.newTransaction();

        // Append the new data files to the table
        final AppendFiles append = icebergTransaction.newAppend();
        dataFiles.forEach(append::appendFile);
        append.commit();

        // Commit the transaction, creating new snapshot
        icebergTransaction.commitTransaction();
    }

    /**
     * Generate a list of {@link DataFile} objects from a list of parquet files written.
     */
    private List<DataFile> dataFilesFromParquet(
            @NotNull final List<CompletedParquetWrite> parquetFilesWritten,
            @NotNull final List<PartitionData> partitionDataList) {
        final int numFiles = parquetFilesWritten.size();
        final List<DataFile> dataFiles = new ArrayList<>(numFiles);
        final PartitionSpec partitionSpec = tableSpec;
        for (int idx = 0; idx < numFiles; idx++) {
            final CompletedParquetWrite completedWrite = parquetFilesWritten.get(idx);
            final DataFiles.Builder dataFileBuilder = DataFiles.builder(partitionSpec)
                    .withPath(completedWrite.destination().toString())
                    .withFormat(FileFormat.PARQUET)
                    .withRecordCount(completedWrite.numRows())
                    .withFileSizeInBytes(completedWrite.numBytes())
                    .withSortOrder(sortOrderToWrite);
            if (partitionSpec.isPartitioned()) {
                dataFileBuilder.withPartition(partitionDataList.get(idx));
            }
            dataFiles.add(dataFileBuilder.build());
        }
        return dataFiles;
    }
}
