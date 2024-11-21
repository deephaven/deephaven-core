//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.parquet.table.CompletedParquetWrite;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.iceberg.util.SchemaSpecInternal.SchemaSpecImpl;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.deephaven.iceberg.base.IcebergUtils.allDataFiles;
import static io.deephaven.iceberg.base.IcebergUtils.partitionDataFromPaths;
import static io.deephaven.iceberg.base.IcebergUtils.verifyPartitioningColumns;
import static io.deephaven.iceberg.base.IcebergUtils.verifyRequiredFields;

/**
 * This class is responsible for writing Deephaven tables to an Iceberg table. Each instance of this class is associated
 * with a single {@link IcebergTableAdapter} and can be used to write multiple Deephaven tables to this Iceberg table.
 */
public class IcebergTableWriter {

    /**
     * The Iceberg table adapter and table which will be written to by this instance.
     */
    private final IcebergTableAdapter tableAdapter;
    private final org.apache.iceberg.Table table;

    @Nullable
    private final TableMetadata tableMetadata;

    /**
     * The table definition used for all writes by this writer instance.
     */
    private final TableDefinition tableDefinition;

    /**
     * The schema to use when in conjunction with the {@link #fieldIdToColumnName} to map Deephaven columns from
     * {@link #tableDefinition} to Iceberg columns.
     */
    private final Schema userSchema;

    /**
     * Mapping from Iceberg field IDs to Deephaven column names, populated inside the parquet file.
     */
    private final Map<Integer, String> fieldIdToColumnName;

    /**
     * Initialized lazily from the {@value TableProperties#DEFAULT_NAME_MAPPING} property in the table metadata, if
     * needed.
     */
    @Nullable
    private Map<String, Integer> nameMappingDefault;

    /**
     * The factory to create new output file locations for writing data files.
     */
    private final OutputFileFactory outputFileFactory;


    IcebergTableWriter(final TableWriterOptions tableWriterOptions, final IcebergTableAdapter tableAdapter) {
        this.tableAdapter = tableAdapter;
        this.table = tableAdapter.icebergTable();

        if (table instanceof HasTableOperations) {
            tableMetadata = ((HasTableOperations) table).operations().current();
        } else {
            tableMetadata = null;
        }

        this.tableDefinition = tableWriterOptions.tableDefinition();
        verifyRequiredFields(table.schema(), tableDefinition);
        verifyPartitioningColumns(table.spec(), tableDefinition);

        this.userSchema = ((SchemaSpecImpl) tableWriterOptions.schemaSpec()).getSchema(table);
        verifyFieldIdsInSchema(tableWriterOptions.fieldIdToColumnName().keySet(), userSchema);

        // Create a copy of the fieldIdToColumnName map since we might need to add new entries for columns which are not
        // provided by the user.
        this.fieldIdToColumnName = new HashMap<>(tableWriterOptions.fieldIdToColumnName());
        addFieldIdsForAllColumns(tableWriterOptions);

        outputFileFactory = OutputFileFactory.builderFor(table, 0, 0)
                .format(FileFormat.PARQUET)
                .build();
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
    private void addFieldIdsForAllColumns(final TableWriterOptions tableWriterOptions) {
        final Map<String, Integer> dhColumnNameToFieldId = tableWriterOptions.dhColumnNameToFieldId();
        for (final ColumnDefinition<?> columnDefinition : tableDefinition.getColumns()) {
            final String columnName = columnDefinition.getName();

            // We are done if we already have the mapping between column name and field ID
            if (dhColumnNameToFieldId.containsKey(columnName)) {
                continue;
            }

            // To be populated by the end of this block for each column, else throw an exception
            Integer fieldId = null;
            Types.NestedField nestedField;

            // Check in the schema.name_mapping.default map
            fieldId = lazyNameMappingDefault().get(columnName);
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

            if (fieldId == null) {
                throw new IllegalArgumentException("Column " + columnName + " not found in the schema or " +
                        "the name mapping for the table");
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
    private Map<String, Integer> lazyNameMappingDefault() {
        if (nameMappingDefault != null) {
            return nameMappingDefault;
        }
        if (tableMetadata == null) {
            return nameMappingDefault = Map.of();
        }
        final String nameMappingJson = tableMetadata.property(TableProperties.DEFAULT_NAME_MAPPING, null);
        if (nameMappingJson == null) {
            return nameMappingDefault = Map.of();
        }
        // Iterate over all mapped fields and build a reverse map from column name to field ID
        nameMappingDefault = new HashMap<>();
        final NameMapping nameMapping = NameMappingParser.fromJson(nameMappingJson);
        for (final MappedField field : nameMapping.asMappedFields().fields()) {
            final Integer fieldId = field.id();
            for (final String name : field.names()) {
                nameMappingDefault.put(name, fieldId);
            }
        }
        return nameMappingDefault;
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
        commit(dataFilesWritten, false, writeInstructions);
    }

    /**
     * Overwrite the existing Iceberg table with the provided Deephaven {@link IcebergWriteInstructions#tables()} in a
     * single snapshot. This will delete all existing data and will not change the schema of the existing table. This
     * method will not perform any compatibility checks between the existing schema and the provided Deephaven tables.
     * <p>
     * Overwriting a table while racing with other writers can lead to failure/undefined results.
     *
     * @param writeInstructions The instructions for customizations while writing.
     */
    public void overwrite(@NotNull final IcebergWriteInstructions writeInstructions) {
        final List<DataFile> dataFilesWritten = writeDataFiles(writeInstructions);
        commit(dataFilesWritten, true, writeInstructions);
    }

    /**
     * Writes data from Deephaven {@link IcebergWriteInstructions#tables()} to an Iceberg table without creating a new
     * snapshot. This method returns a list of data files that were written. Users can use this list to create a
     * transaction/snapshot if needed. This method will not perform any compatibility checks between the existing schema
     * and the provided Deephaven tables.
     *
     * @param instructions The instructions for customizations while writing.
     */
    public List<DataFile> writeDataFiles(@NotNull final IcebergWriteInstructions instructions) {
        final IcebergParquetWriteInstructions writeInstructions = verifyInstructions(instructions);
        // Verify that the table definition matches the Iceberg table writer
        if (writeInstructions.tableDefinition().isPresent() &&
                !writeInstructions.tableDefinition().get().equals(tableDefinition)) {
            throw new IllegalArgumentException(
                    "Failed to write data to Iceberg table. The provided table definition does not match the " +
                            "table definition of the Iceberg table writer. Table definition provided : " +
                            writeInstructions.tableDefinition().get() + ", table definition of the Iceberg " +
                            "table writer : " + tableDefinition);
        }

        final List<String> partitionPaths = writeInstructions.partitionPaths();
        verifyPartitionPaths(table, partitionPaths);
        final Pair<List<PartitionData>, List<String[]>> ret = partitionDataFromPaths(table.spec(), partitionPaths);
        final List<PartitionData> partitionData = ret.getFirst();
        final List<String[]> dhTableUpdateStrings = ret.getSecond();
        final List<CompletedParquetWrite> parquetFileInfo =
                writeParquet(partitionData, dhTableUpdateStrings, writeInstructions);
        return dataFilesFromParquet(parquetFileInfo, partitionData);
    }

    static IcebergParquetWriteInstructions verifyInstructions(
            @NotNull final IcebergWriteInstructions instructions) {
        // We ony support writing to Parquet files
        if (!(instructions instanceof IcebergParquetWriteInstructions)) {
            throw new IllegalArgumentException("Unsupported instructions of class " + instructions.getClass() + " for" +
                    " writing Iceberg table, expected: " + IcebergParquetWriteInstructions.class);
        }
        return (IcebergParquetWriteInstructions) instructions;
    }

    private static void verifyPartitionPaths(
            final org.apache.iceberg.Table icebergTable,
            final Collection<String> partitionPaths) {
        if (icebergTable.spec().isPartitioned() && partitionPaths.isEmpty()) {
            throw new IllegalArgumentException("Cannot write data to a partitioned table without partition paths.");
        }
        if (!icebergTable.spec().isPartitioned() && !partitionPaths.isEmpty()) {
            throw new IllegalArgumentException("Cannot write data to an un-partitioned table with partition paths.");
        }
    }

    @NotNull
    private List<CompletedParquetWrite> writeParquet(
            @NotNull final List<PartitionData> partitionDataList,
            @NotNull final List<String[]> dhTableUpdateStrings,
            @NotNull final IcebergParquetWriteInstructions writeInstructions) {
        final List<Table> dhTables = writeInstructions.tables();
        final boolean isPartitioned = table.spec().isPartitioned();
        if (isPartitioned) {
            Require.eq(dhTables.size(), "dhTables.size()",
                    partitionDataList.size(), "partitionDataList.size()");
            Require.eq(dhTables.size(), "dhTables.size()",
                    dhTableUpdateStrings.size(), "dhTableUpdateStrings.size()");
        }

        // Build the parquet instructions
        final List<CompletedParquetWrite> parquetFilesWritten = new ArrayList<>(dhTables.size());
        final ParquetInstructions.OnWriteCompleted onWriteCompleted = parquetFilesWritten::add;
        final ParquetInstructions parquetInstructions = writeInstructions.toParquetInstructions(
                onWriteCompleted, tableDefinition, fieldIdToColumnName);

        // Write the data to parquet files
        for (int idx = 0; idx < dhTables.size(); idx++) {
            Table dhTable = dhTables.get(idx);
            if (dhTable.numColumns() == 0) {
                // Skip writing empty tables with no columns
                continue;
            }
            final String newDataLocation;
            if (isPartitioned) {
                newDataLocation = getDataLocation(partitionDataList.get(idx));
                dhTable = dhTable.updateView(dhTableUpdateStrings.get(idx));
            } else {
                newDataLocation = getDataLocation();
            }
            // TODO (deephaven-core#6343): Set writeDefault() values for required columns that not present in the table
            ParquetTools.writeTable(dhTable, newDataLocation, parquetInstructions);
        }
        return parquetFilesWritten;
    }

    /**
     * Generate the location string for a new data file for the given partition data.
     */
    private String getDataLocation(@NotNull final PartitionData partitionData) {
        final EncryptedOutputFile outputFile = outputFileFactory.newOutputFile(table.spec(), partitionData);
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
    private void commit(
            @NotNull final Iterable<DataFile> dataFiles,
            final boolean overwrite,
            @NotNull final IcebergBaseInstructions writeInstructions) {
        final Transaction icebergTransaction = table.newTransaction();
        final Snapshot referenceSnapshot;
        {
            final Snapshot snapshotFromInstructions = tableAdapter.getSnapshot(writeInstructions);
            if (snapshotFromInstructions != null) {
                referenceSnapshot = snapshotFromInstructions;
            } else {
                referenceSnapshot = table.currentSnapshot();
            }
        }
        if (overwrite) {
            // Fail if the table gets changed concurrently
            final OverwriteFiles overwriteFiles = icebergTransaction.newOverwrite()
                    .validateFromSnapshot(referenceSnapshot.snapshotId())
                    .validateNoConflictingDeletes()
                    .validateNoConflictingData();

            // Delete all the existing data files in the table
            try (final Stream<DataFile> existingDataFiles = allDataFiles(table, referenceSnapshot)) {
                existingDataFiles.forEach(overwriteFiles::deleteFile);
            }
            dataFiles.forEach(overwriteFiles::addFile);
            overwriteFiles.commit();
        } else {
            // Append the new data files to the table
            final AppendFiles append = icebergTransaction.newAppend();
            dataFiles.forEach(append::appendFile);
            append.commit();
        }

        // Commit the transaction, creating new snapshot for append/overwrite.
        // Note that no new snapshot will be created for the schema change.
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
        final PartitionSpec partitionSpec = table.spec();
        for (int idx = 0; idx < numFiles; idx++) {
            final CompletedParquetWrite completedWrite = parquetFilesWritten.get(idx);
            final DataFiles.Builder dataFileBuilder = DataFiles.builder(partitionSpec)
                    .withPath(completedWrite.destination().toString())
                    .withFormat(FileFormat.PARQUET)
                    .withRecordCount(completedWrite.numRows())
                    .withFileSizeInBytes(completedWrite.numBytes());
            if (partitionSpec.isPartitioned()) {
                dataFileBuilder.withPartition(partitionDataList.get(idx));
            }
            dataFiles.add(dataFileBuilder.build());
        }
        return dataFiles;
    }
}
