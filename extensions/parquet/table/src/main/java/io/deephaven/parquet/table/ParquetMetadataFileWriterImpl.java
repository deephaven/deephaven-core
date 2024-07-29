//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.parquet.base.ParquetFileWriter;
import io.deephaven.parquet.base.ParquetMetadataFileWriter;
import io.deephaven.parquet.base.ParquetUtils;
import io.deephaven.parquet.table.metadata.ColumnTypeInfo;
import io.deephaven.parquet.table.metadata.TableInfo;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static io.deephaven.parquet.base.ParquetUtils.MAGIC;
import static io.deephaven.parquet.base.ParquetUtils.METADATA_KEY;
import static io.deephaven.parquet.base.ParquetUtils.PARQUET_OUTPUT_BUFFER_SIZE;
import static io.deephaven.parquet.base.ParquetUtils.getPerFileMetadataKey;

/**
 * Used to generate a combined {@value ParquetUtils#METADATA_FILE_NAME} and
 * {@value ParquetUtils#COMMON_METADATA_FILE_NAME} file for provided Parquet files. This class is stateful and therefore
 * should not be used by multiple threads concurrently.
 */
final class ParquetMetadataFileWriterImpl implements ParquetMetadataFileWriter {

    /**
     * A class to hold the parquet file and its metadata.
     */
    private static class ParquetFileMetadata {
        final URI uri;
        final ParquetMetadata metadata;

        ParquetFileMetadata(final URI uri, final ParquetMetadata metadata) {
            this.uri = uri;
            this.metadata = metadata;
        }
    }

    private final URI metadataRootDir;
    private final List<ParquetFileMetadata> parquetFileMetadataList;
    private final SeekableChannelsProvider channelsProvider;
    private final MessageType partitioningColumnsSchema;

    // The following fields are used to accumulate metadata for all parquet files
    private MessageType mergedSchema;
    private String mergedCreatedByString;
    private final Map<String, String> mergedKeyValueMetaData;
    private final List<BlockMetaData> mergedBlocks;
    /**
     * Per-column type information stored in key-value metadata
     */
    private List<ColumnTypeInfo> mergedColumnTypes;
    private String mergedVersion;

    /**
     * @param metadataRootDir The root directory for the metadata files
     * @param destinations The individual parquet file destinations, all of which must be contained in the metadata root
     * @param partitioningColumnsSchema The common schema for partitioning columns to be included in the
     *        {@value ParquetUtils#COMMON_METADATA_FILE_NAME} file, can be null if there are no partitioning columns.
     */
    ParquetMetadataFileWriterImpl(
            @NotNull final URI metadataRootDir,
            @NotNull final URI[] destinations,
            @Nullable final MessageType partitioningColumnsSchema,
            @NotNull final ParquetInstructions writeInstructions) {
        if (destinations.length == 0) {
            throw new IllegalArgumentException("No destinations provided");
        }
        this.metadataRootDir = metadataRootDir;
        final String metadataRootDirStr = metadataRootDir.toString();
        for (final URI destination : destinations) {
            if (!destination.toString().startsWith(metadataRootDirStr)) {
                throw new UncheckedDeephavenException("All destinations must be nested under the provided metadata root"
                        + " directory, provided destination " + destination + " is not under " + metadataRootDir);
            }
        }
        this.parquetFileMetadataList = new ArrayList<>(destinations.length);
        this.channelsProvider = SeekableChannelsProviderLoader.getInstance().fromServiceLoader(metadataRootDir,
                writeInstructions.getSpecialInstructions());
        this.partitioningColumnsSchema = partitioningColumnsSchema;

        this.mergedSchema = null;
        this.mergedCreatedByString = null;
        this.mergedKeyValueMetaData = new HashMap<>();
        this.mergedBlocks = new ArrayList<>();
        this.mergedColumnTypes = null;
        this.mergedVersion = null;
    }

    /**
     * Add parquet metadata for the provided parquet file to the combined metadata file.
     *
     * @param parquetFileURI The parquet file destination URI
     * @param metadata The parquet metadata
     */
    public void addParquetFileMetadata(final URI parquetFileURI, final ParquetMetadata metadata) {
        parquetFileMetadataList.add(new ParquetFileMetadata(parquetFileURI, metadata));
    }

    /**
     * Write the accumulated metadata to the provided files and clear the metadata accumulated so far.
     *
     * @param metadataFileURI The destination URI for the {@value ParquetUtils#METADATA_FILE_NAME} file
     * @param commonMetadataFileURI The destination URI for the {@value ParquetUtils#COMMON_METADATA_FILE_NAME} file
     */
    public void writeMetadataFiles(final URI metadataFileURI, final URI commonMetadataFileURI)
            throws IOException {
        if (parquetFileMetadataList.isEmpty()) {
            throw new UncheckedDeephavenException("No parquet files to write metadata for");
        }
        mergeMetadata();
        final ParquetMetadata metadataFooter = new ParquetMetadata(new FileMetaData(mergedSchema,
                mergedKeyValueMetaData, mergedCreatedByString), mergedBlocks);
        writeMetadataFile(metadataFooter, metadataFileURI);

        // Skip the blocks data and merge schema with partitioning columns' schema to write the common metadata file.
        // The ordering of arguments in method call is important because we want to keep partitioning columns in the
        // beginning.
        mergedSchema = mergeSchemaInto(mergedSchema, partitioningColumnsSchema);
        final ParquetMetadata commonMetadataFooter =
                new ParquetMetadata(new FileMetaData(mergedSchema, mergedKeyValueMetaData, mergedCreatedByString),
                        new ArrayList<>());
        writeMetadataFile(commonMetadataFooter, commonMetadataFileURI);

        // Clear the accumulated metadata
        clear();
    }

    /**
     * Merge all the accumulated metadata for the parquet files.
     */
    private void mergeMetadata() throws IOException {
        final Collection<String> mergedCreatedBy = new HashSet<>();
        for (final ParquetFileMetadata parquetFileMetadata : parquetFileMetadataList) {
            final FileMetaData fileMetaData = parquetFileMetadata.metadata.getFileMetaData();
            mergedSchema = mergeSchemaInto(fileMetaData.getSchema(), mergedSchema);
            final String relativePath = metadataRootDir.relativize(parquetFileMetadata.uri).getPath();
            mergeKeyValueMetaData(parquetFileMetadata, relativePath);
            mergeBlocksInto(parquetFileMetadata, relativePath, mergedBlocks);
            mergedCreatedBy.add(fileMetaData.getCreatedBy());
        }
        if (mergedKeyValueMetaData.size() != parquetFileMetadataList.size()) {
            throw new IllegalStateException("We should have one entry for each file in the merged key-value metadata, "
                    + "but we have " + mergedKeyValueMetaData.size() + " entries for " + parquetFileMetadataList.size()
                    + " files.");
        }
        // Add table info to the merged key-value metadata
        final TableInfo.Builder tableInfoBuilder = TableInfo.builder().addAllColumnTypes(mergedColumnTypes);
        if (mergedVersion != null) {
            tableInfoBuilder.version(mergedVersion);
        }
        mergedKeyValueMetaData.put(METADATA_KEY, tableInfoBuilder.build().serializeToJSON());
        mergedCreatedByString =
                mergedCreatedBy.size() == 1 ? mergedCreatedBy.iterator().next() : mergedCreatedBy.toString();
    }

    /**
     * Merge the provided schema into the merged schema. Note that if there are common fields between the two schemas,
     * the output schema will have the fields in the order they appear in the merged schema.
     */
    private static MessageType mergeSchemaInto(final MessageType schema, final MessageType mergedSchema) {
        if (mergedSchema == null) {
            return schema;
        }
        if (mergedSchema.equals(schema)) {
            return mergedSchema;
        }
        return mergedSchema.union(schema, true);
    }

    /**
     * This method processes both deephaven specific and non-deephaven key-value metadata for each file.
     * <ul>
     * <li>For non-deephaven specific key-value metadata, we accumulate it directly and enforce that there is only one
     * value for each key</li>
     * <li>For deephaven specific key-value metadata, we copy each file's metadata directly into the merged metadata as
     * well as accumulate the required fields to generate a common table info later once all files are processed.</li>
     * </ul>
     */
    private void mergeKeyValueMetaData(@NotNull final ParquetFileMetadata parquetFileMetadata,
            @NotNull final String relativePath) throws IOException {
        final Map<String, String> keyValueMetaData =
                parquetFileMetadata.metadata.getFileMetaData().getKeyValueMetaData();
        for (final Map.Entry<String, String> entry : keyValueMetaData.entrySet()) {
            if (!entry.getKey().equals(METADATA_KEY)) {
                // Make sure we have unique value for each key.
                mergedKeyValueMetaData.compute(entry.getKey(), (k, v) -> {
                    if (v == null) {
                        // No existing value for this key, so put the new value
                        return entry.getValue();
                    } else if (!v.equals(entry.getValue())) {
                        // Existing value does not match the new value
                        throw new UncheckedDeephavenException("Could not merge metadata for key " + entry.getKey() +
                                ", has conflicting values: " + entry.getValue() + " and " + v);
                    }
                    // Existing value matches the new value, no action needed
                    return v;
                });
            } else {
                // Add a separate entry for each file
                final String fileKey = getPerFileMetadataKey(relativePath);
                // Assuming the keys are unique for each file because file names are unique, verified in the constructor
                if (mergedKeyValueMetaData.containsKey(fileKey)) {
                    throw new IllegalStateException("Could not merge metadata for file " +
                            parquetFileMetadata.uri + " because it has conflicting file key: " + fileKey);
                }
                mergedKeyValueMetaData.put(fileKey, entry.getValue());

                // Also, process and accumulate the relevant fields:
                // - groupingColumns, dataIndexes are skipped
                // - columnTypes must be the same for all partitions
                // - version is set as non-null if all the files have the same version
                final TableInfo tableInfo = TableInfo.deserializeFromJSON(entry.getValue());
                if (mergedColumnTypes == null) {
                    // The First file for which we've seen deephaven specific metadata, so just copy the relevant fields
                    mergedColumnTypes = tableInfo.columnTypes();
                    mergedVersion = tableInfo.version();
                } else {
                    if (!mergedColumnTypes.equals(tableInfo.columnTypes())) {
                        throw new UncheckedDeephavenException("Could not merge metadata for key " + METADATA_KEY +
                                ", has conflicting values for columnTypes: " + tableInfo.columnTypes() + " and "
                                + mergedColumnTypes);
                    }
                    if (!tableInfo.version().equals(mergedVersion)) {
                        mergedVersion = null;
                    }
                }
            }
        }
    }

    private static void mergeBlocksInto(final ParquetFileMetadata parquetFileMetadata,
            final String fileRelativePathString, final Collection<BlockMetaData> mergedBlocks) {
        for (final BlockMetaData block : parquetFileMetadata.metadata.getBlocks()) {
            block.setPath(fileRelativePathString);
            mergedBlocks.add(block);
        }
    }

    private void writeMetadataFile(final ParquetMetadata metadataFooter, final URI dest) throws IOException {
        final CountingOutputStream metadataOutputStream =
                new CountingOutputStream(channelsProvider.getOutputStream(dest, false, PARQUET_OUTPUT_BUFFER_SIZE));
        metadataOutputStream.write(MAGIC);
        ParquetFileWriter.serializeFooter(metadataFooter, metadataOutputStream);
        metadataOutputStream.close();
    }

    public void clear() {
        parquetFileMetadataList.clear();
        mergedKeyValueMetaData.clear();
        mergedBlocks.clear();
        mergedColumnTypes = null;
        mergedSchema = null;
        mergedCreatedByString = null;
    }
}
