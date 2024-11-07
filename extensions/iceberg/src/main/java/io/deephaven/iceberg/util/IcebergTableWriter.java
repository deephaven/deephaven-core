//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
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
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.deephaven.iceberg.base.IcebergUtils.allDataFiles;
import static io.deephaven.iceberg.base.IcebergUtils.convertToDHType;
import static io.deephaven.iceberg.base.IcebergUtils.convertToIcebergType;
import static io.deephaven.iceberg.base.IcebergUtils.createPartitionSpec;
import static io.deephaven.iceberg.base.IcebergUtils.partitionDataFromPaths;
import static io.deephaven.iceberg.base.IcebergUtils.verifyWriteCompatibility;
import static org.apache.iceberg.types.TypeUtil.isPromotionAllowed;

public class IcebergTableWriter {

    /**
     * The table definition provided by user to write to the Iceberg table. All deephaven tables written by this writer
     * instance are expected to have this definition.
     */
    private final TableDefinition tableDefinition;

    /**
     * The table definition used for writing the Parquet file. This differs from {@link #tableDefinition} as it:
     * <ul>
     * <li>Excludes the partitioning columns</li>
     * <li>Includes type promotions needed to make {@link #tableDefinition} compatible with the existing table</li>
     * </ul>
     */
    private TableDefinition parquetTableDefinition;

    /**
     * A one-to-one {@link Map map} from Deephaven column names from the {@link #tableDefinition} to Iceberg field IDs
     * from the {@link #userSchema}.
     */
    private final Map<String, Integer> dhColumnsToIcebergFieldIds;

    /**
     * Reverse mapping from Iceberg field IDs to Deephaven column names.
     */
    private final Map<Integer, String> icebergFieldIdToDhColumn;

    /**
     * The schema to use when in conjunction with the {@link #dhColumnsToIcebergFieldIds} to map Deephaven columns from
     * {@link #tableDefinition} to Iceberg columns.
     */
    private final Schema userSchema;

    private final IcebergTableAdapter tableAdapter;
    private final org.apache.iceberg.Table table;

    @Nullable
    private final TableMetadata tableMetadata;
    @Nullable
    private Map<String, Integer> nameMappingDefault;

    private Schema schemaToWrite;
    private PartitionSpec partitionSpecToWrite;

    private OutputFileFactory outputFileFactory;

    IcebergTableWriter(
            final TableWriterOptions tableWriterOptions,
            final IcebergTableAdapter tableAdapter) {
        this.tableDefinition = tableWriterOptions.tableDefinition();
        this.dhColumnsToIcebergFieldIds = tableWriterOptions.dhColumnsToIcebergFieldIds();
        this.icebergFieldIdToDhColumn = new HashMap<>();
        this.table = tableAdapter.icebergTable();
        this.userSchema = tableWriterOptions.schema().orElseGet(table::schema);
        verifyFieldIds(dhColumnsToIcebergFieldIds.values(), userSchema);
        this.tableAdapter = tableAdapter;

        if (table instanceof HasTableOperations) {
            tableMetadata = ((HasTableOperations) table).operations().current();
        } else {
            tableMetadata = null;
        }
        inferSpecAndSchemaToWrite();
    }

    /**
     * Check that all the field IDs are present in the schema.
     */
    private void verifyFieldIds(final Collection<Integer> fieldIds, final Schema schema) {
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
     * Pre-compute the schema and partition spec to use for writing the tables for this instance. This will save time
     * during write operation.
     */
    private void inferSpecAndSchemaToWrite() {
        // List to hold the new fields for the inferred schema
        final List<Types.NestedField> inferredSchemaFields = new ArrayList<>();

        final int lastColumnId;
        if (tableMetadata != null) {
            lastColumnId = tableMetadata.lastColumnId();
        } else {
            lastColumnId = table.schema().highestFieldId();
        }
        // AtomicInteger to generate new field IDs starting from the lastColumnId
        final AtomicInteger columnIdGenerator = new AtomicInteger(lastColumnId);

        // Collect the partitioning columns to build partition spec
        final Collection<String> partitioningColumns = new ArrayList<>();

        // Column definition to be written to the parquet file
        final Collection<ColumnDefinition<?>> parquetColumnDefinitions = new ArrayList<>(tableDefinition.numColumns());

        // Iterate through each column in the table definition and build the new spec and schema
        for (final ColumnDefinition<?> columnDefinition : tableDefinition.getColumns()) {
            final String columnName = columnDefinition.getName();
            Types.NestedField nestedField = null;

            // Check in the column_name->Field_ID map
            if (dhColumnsToIcebergFieldIds.containsKey(columnName)) {
                final int fieldId = dhColumnsToIcebergFieldIds.get(columnName);
                nestedField = userSchema.findField(fieldId);
                // Assuming nestedField is not null, as we have already verified above
            }

            // Check in the schema.name_mapping.default map
            if (nestedField == null) {
                final Integer fieldId = lazyNameMappingDefault().get(columnName);
                if (fieldId != null) {
                    nestedField = userSchema.findField(fieldId);
                    if (nestedField == null) {
                        throw new IllegalArgumentException("Field ID " + fieldId + " extracted for column " +
                                columnName + " from the schema.name_mapping map not found in schema " + userSchema);
                    }
                }
            }

            // Directly lookup in the user provided schema using column name
            if (nestedField == null) {
                nestedField = userSchema.findField(columnName);
            }

            // The column definition to be used for writing the parquet file
            final ColumnDefinition<?> parquetColumnDefinition;

            if (nestedField == null) {
                // We couldn't find the field in the schema, so we assign a new field ID and use the name and type from
                // the table definition
                final int newFieldId = columnIdGenerator.incrementAndGet();
                nestedField = Types.NestedField.of(newFieldId, true, columnName,
                        convertToIcebergType(columnDefinition.getDataType()));
                parquetColumnDefinition = columnDefinition;
            } else {
                // Field was found in the schema, so we will derive the type from the schema.
                // But first, need to check if the type from schema is assignable from the table definition
                if (!isTypeAssignable(convertToIcebergType(columnDefinition.getDataType()), nestedField.type())) {
                    throw new IllegalArgumentException("Cannot write data from deephaven column type " +
                            columnDefinition.getDataType() + " to iceberg column type " + nestedField.type());
                }

                // Convert the schema type to Deephaven type and create a new column definition for the parquet file.
                // The actual promoting of values will be handled by the parquet writing code based on this column
                // definition.
                final io.deephaven.qst.type.Type<?> dhTypeToWrite = convertToDHType(nestedField.type());
                parquetColumnDefinition = ColumnDefinition.of(columnName, dhTypeToWrite);
            }
            inferredSchemaFields.add(nestedField);

            // Store the mapping from field ID to column name to be populated inside the parquet file
            icebergFieldIdToDhColumn.put(nestedField.fieldId(), columnName);

            if (columnDefinition.isPartitioning()) {
                partitioningColumns.add(nestedField.name());
            } else {
                // We only write non-partitioning columns to parquet file
                parquetColumnDefinitions.add(parquetColumnDefinition);
            }
        }
        this.schemaToWrite = new Schema(inferredSchemaFields);
        this.partitionSpecToWrite = createPartitionSpec(schemaToWrite, partitioningColumns);
        this.parquetTableDefinition = TableDefinition.of(parquetColumnDefinitions);
    }

    private static boolean isTypeAssignable(final Type fromType, final Type toType) {
        if (fromType.equals(toType)) {
            return true;
        }
        if (fromType.isPrimitiveType() && toType.isPrimitiveType()) {
            return isPromotionAllowed(fromType.asPrimitiveType(), toType.asPrimitiveType());
        }
        return false;
    }

    /**
     * Build the mapping from column names to field IDs on demand using the
     * {@value TableProperties#DEFAULT_NAME_MAPPING} map.
     */
    private Map<String, Integer> lazyNameMappingDefault() {
        if (nameMappingDefault == null) {
            if (tableMetadata != null) {
                final String nameMappingJson = tableMetadata.property(TableProperties.DEFAULT_NAME_MAPPING, null);
                if (nameMappingJson != null) {
                    nameMappingDefault = new HashMap<>();
                    final NameMapping nameMapping = NameMappingParser.fromJson(nameMappingJson);
                    // Iterate over all mapped fields and build a reverse map from column name to field ID
                    for (final MappedField field : nameMapping.asMappedFields().fields()) {
                        final Integer fieldId = field.id();
                        for (final String name : field.names()) {
                            nameMappingDefault.put(name, fieldId);
                        }
                    }
                } else {
                    nameMappingDefault = Map.of();
                }
            } else {
                nameMappingDefault = Map.of();
            }
        }
        return nameMappingDefault;
    }

    /**
     * Append the provided Deephaven table as a new partition to the existing Iceberg table in a single snapshot. This
     * will not change the schema of the existing table.
     *
     * @param writeInstructions The instructions for customizations while writing.
     */
    public void append(@NotNull final IcebergWriteInstructions writeInstructions) {
        writeImpl(writeInstructions, false, true);
    }

    /**
     * Overwrite the existing Iceberg table with the provided Deephaven tables in a single snapshot. This will overwrite
     * the schema of the existing table to match the provided Deephaven table if they do not match.
     * <p>
     * Overwriting a table while racing with other writers can lead to failure/undefined results.
     *
     * @param writeInstructions The instructions for customizations while writing.
     */
    public void overwrite(@NotNull final IcebergWriteInstructions writeInstructions) {
        writeImpl(writeInstructions, true, true);
    }

    /**
     * Writes data from Deephaven tables to an Iceberg table without creating a new snapshot. This method returns a list
     * of data files that were written. Users can use this list to create a transaction/snapshot if needed.
     *
     * @param writeInstructions The instructions for customizations while writing.
     */
    public List<DataFile> writeDataFiles(@NotNull final IcebergWriteInstructions writeInstructions) {
        return writeImpl(writeInstructions, false, false);
    }

    /**
     * Appends or overwrites data in an Iceberg table with the provided Deephaven tables.
     *
     * @param instructions The instructions for customizations while writing, or null to use default instructions
     * @param overwrite If true, the existing data in the Iceberg table will be overwritten; if false, the data will be
     *        appended
     * @param addSnapshot If true, a new snapshot will be created in the Iceberg table with the written data
     *
     * @return A list of DataFile objects representing the written data files.
     */
    private List<DataFile> writeImpl(
            @NotNull final IcebergWriteInstructions instructions,
            final boolean overwrite,
            final boolean addSnapshot) {
        if (overwrite && !addSnapshot) {
            throw new IllegalArgumentException("Cannot overwrite an Iceberg table without adding a snapshot");
        }
        IcebergParquetWriteInstructions writeInstructions = verifyInstructions(instructions);
        List<Table> dhTables = instructions.dhTables();
        if (dhTables.isEmpty()) {
            if (!overwrite) {
                // Nothing to append
                return Collections.emptyList();
            }
            // Overwrite with an empty table
            dhTables = List.of(TableTools.emptyTable(0));
            writeInstructions = writeInstructions.withDhTables(dhTables);
        }

        // Verify that the table definition matches the Iceberg table writer
        if (writeInstructions.tableDefinition().isPresent()) {
            if (!writeInstructions.tableDefinition().get().equals(tableDefinition)) {
                throw new IllegalArgumentException(
                        "Failed to write data to Iceberg table. The provided table definition does not match the " +
                                "table definition of the Iceberg table writer. Table definition provided : " +
                                writeInstructions.tableDefinition().get() + ", table definition of the Iceberg " +
                                "table writer : " + tableDefinition);
            }
        } else {
            writeInstructions = writeInstructions.withTableDefinition(tableDefinition);
        }

        // Verify that the schema and partition spec are compatible with the Iceberg table
        // Note that we do not support updating the partition spec since the iceberg reading code doesn't support it
        if (!partitionSpecToWrite.compatibleWith(table.spec())) {
            throw new IllegalArgumentException("Partition spec of the iceberg table is not compatible with the " +
                    "partition spec to be written. Table partition spec : " + table.spec() +
                    ", Iceberg table writer partition spec : " + partitionSpecToWrite);
        }
        if (!instructions.updateSchema()) {
            try {
                verifyWriteCompatibility(table.schema(), schemaToWrite);
            } catch (final IllegalArgumentException exception) {
                throw new IllegalArgumentException("Schema of the iceberg table is not compatible with the " +
                        "schema to be written. Table schema : " + table.schema() + ", Iceberg table writer schema : " +
                        schemaToWrite + ". Please retry after enabling schema update in the write instructions",
                        exception);
            }
        }

        final List<String> partitionPaths = writeInstructions.partitionPaths();
        verifyPartitionPaths(table, partitionPaths);
        final List<PartitionData> partitionData = partitionDataFromPaths(partitionSpecToWrite, partitionPaths);

        final List<CompletedParquetWrite> parquetFileInfo = writeParquet(dhTables, partitionData, writeInstructions);
        final List<DataFile> appendFiles = dataFilesFromParquet(parquetFileInfo, partitionData);
        if (addSnapshot) {
            commit(appendFiles, overwrite, writeInstructions);
        }
        return appendFiles;
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

    static IcebergParquetWriteInstructions ensureDefinition(
            @NotNull final IcebergParquetWriteInstructions writeInstructions) {
        if (writeInstructions.tableDefinition().isPresent()) {
            return writeInstructions;
        } else {
            // Verify that all tables have the same definition
            final List<Table> dhTables = writeInstructions.dhTables();
            final int numTables = dhTables.size();
            if (numTables == 0) {
                return writeInstructions.withTableDefinition(TableDefinition.of());
            }
            final TableDefinition firstDefinition = dhTables.get(0).getDefinition();
            for (int idx = 1; idx < numTables; idx++) {
                if (!firstDefinition.equals(dhTables.get(idx).getDefinition())) {
                    throw new IllegalArgumentException(
                            "All Deephaven tables must have the same definition, else table definition should be " +
                                    "provided when writing multiple tables with different definitions");
                }
            }
            return writeInstructions.withTableDefinition(firstDefinition);
        }
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

    private static class CompletedParquetWrite {
        private final URI destination;
        private final long numRows;
        private final long numBytes;

        private CompletedParquetWrite(final URI destination, final long numRows, final long numBytes) {
            this.destination = destination;
            this.numRows = numRows;
            this.numBytes = numBytes;
        }
    }

    @NotNull
    private List<CompletedParquetWrite> writeParquet(
            @NotNull final List<Table> dhTables,
            @NotNull final List<PartitionData> partitionDataList,
            @NotNull final IcebergParquetWriteInstructions writeInstructions) {
        // Build the parquet instructions
        final List<CompletedParquetWrite> parquetFilesWritten = new ArrayList<>(dhTables.size());
        final ParquetInstructions.OnWriteCompleted onWriteCompleted =
                (destination, numRows, numBytes) -> parquetFilesWritten
                        .add(new CompletedParquetWrite(destination, numRows, numBytes));
        final ParquetInstructions parquetInstructions = writeInstructions.toParquetInstructions(
                onWriteCompleted, parquetTableDefinition, icebergFieldIdToDhColumn);

        // Write the data to parquet files
        for (int idx = 0; idx < dhTables.size(); idx++) {
            final Table dhTable = dhTables.get(idx);
            if (dhTable.numColumns() == 0) {
                // Skip writing empty tables with no columns
                continue;
            }
            final String newDataLocation;
            if (partitionSpecToWrite.isPartitioned()) {
                newDataLocation = getDataLocation(partitionDataList.get(idx));
            } else {
                newDataLocation = getDataLocation();
            }
            // TODO (deephaven-core#6343): Set writeDefault() values for columns that are not present in the table
            ParquetTools.writeTable(dhTable, newDataLocation, parquetInstructions);
        }
        return parquetFilesWritten;
    }

    /**
     * Get the {@link OutputFileFactory} for the Iceberg table, creating one if it does not exist.
     */
    private OutputFileFactory lazyOutputFileFactory() {
        if (outputFileFactory == null) {
            outputFileFactory = OutputFileFactory.builderFor(table, 0, 0)
                    .format(FileFormat.PARQUET)
                    .build();
        }
        return outputFileFactory;
    }

    /**
     * Generate the location string for a new data file for the given partition data.
     */
    private String getDataLocation(@NotNull final PartitionData partitionData) {
        final EncryptedOutputFile outputFile =
                lazyOutputFileFactory().newOutputFile(partitionSpecToWrite, partitionData);
        return outputFile.encryptingOutputFile().location();
    }

    /**
     * Generate the location string for a new data file for the unpartitioned table.
     */
    private String getDataLocation() {
        final EncryptedOutputFile outputFile = lazyOutputFileFactory().newOutputFile();
        return outputFile.encryptingOutputFile().location();
    }

    /**
     * Commit the changes to the Iceberg table by creating a snapshot.
     */
    private void commit(
            @NotNull final Iterable<DataFile> appendFiles,
            final boolean overwrite,
            @NotNull final IcebergWriteInstructions writeInstructions) {
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
            try (final Stream<DataFile> dataFiles = allDataFiles(table, referenceSnapshot)) {
                dataFiles.forEach(overwriteFiles::deleteFile);
            }
            appendFiles.forEach(overwriteFiles::addFile);
            overwriteFiles.commit();
        } else {
            // Append the new data files to the table
            final AppendFiles append = icebergTransaction.newAppend();
            appendFiles.forEach(append::appendFile);
            append.commit();
        }

        // Update the schema of the existing table.
        if (writeInstructions.updateSchema()) {
            final UpdateSchema updateSchema = icebergTransaction.updateSchema();
            updateSchema.unionByNameWith(schemaToWrite);
            updateSchema.commit();
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
        for (int idx = 0; idx < numFiles; idx++) {
            final CompletedParquetWrite completedWrite = parquetFilesWritten.get(idx);
            final DataFiles.Builder dataFileBuilder = DataFiles.builder(partitionSpecToWrite)
                    .withPath(completedWrite.destination.toString())
                    .withFormat(FileFormat.PARQUET)
                    .withRecordCount(completedWrite.numRows)
                    .withFileSizeInBytes(completedWrite.numBytes);
            if (partitionSpecToWrite.isPartitioned()) {
                dataFileBuilder.withPartition(partitionDataList.get(idx));
            }
            dataFiles.add(dataFileBuilder.build());
        }
        return dataFiles;
    }
}
