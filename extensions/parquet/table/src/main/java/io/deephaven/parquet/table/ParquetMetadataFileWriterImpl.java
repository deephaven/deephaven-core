package io.deephaven.parquet.table;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.parquet.base.ParquetFileWriter;
import io.deephaven.parquet.base.ParquetMetadataFileWriter;
import io.deephaven.parquet.base.ParquetUtils;
import io.deephaven.parquet.base.PositionedBufferedOutputStream;
import io.deephaven.parquet.table.metadata.ColumnTypeInfo;
import io.deephaven.parquet.table.metadata.TableInfo;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static io.deephaven.parquet.base.ParquetUtils.MAGIC;
import static io.deephaven.util.channel.SeekableChannelsProvider.convertToURI;

/**
 * Used to generate a combined {@value ParquetUtils#METADATA_FILE_NAME} and
 * {@value ParquetUtils#COMMON_METADATA_FILE_NAME} file for provided Parquet files.
 */
final class ParquetMetadataFileWriterImpl implements ParquetMetadataFileWriter {

    /**
     * A class to hold the parquet file and its metadata.
     */
    private static class ParquetFileMetadata {
        final File file;
        final ParquetMetadata metadata;

        ParquetFileMetadata(final File file, final ParquetMetadata metadata) {
            this.file = file;
            this.metadata = metadata;
        }
    }

    private final Path metadataRootDirAbsPath;
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
    ParquetMetadataFileWriterImpl(@NotNull final File metadataRootDir, @NotNull final File[] destinations,
            @Nullable final MessageType partitioningColumnsSchema) {
        this.metadataRootDirAbsPath = metadataRootDir.getAbsoluteFile().toPath();
        final String metadataRootDirAbsPathString = metadataRootDirAbsPath.toString();
        for (final File destination : destinations) {
            if (!destination.getAbsolutePath().startsWith(metadataRootDirAbsPathString)) {
                throw new UncheckedDeephavenException("All destinations must be nested under the provided metadata root"
                        + " directory, provided destination " + destination.getAbsolutePath() + " is not under " +
                        metadataRootDirAbsPathString);
            }
        }
        this.parquetFileMetadataList = new ArrayList<>(destinations.length);
        this.channelsProvider = SeekableChannelsProviderLoader.getInstance().fromServiceLoader(
                convertToURI(metadataRootDirAbsPathString), null);
        this.partitioningColumnsSchema = partitioningColumnsSchema;

        this.mergedSchema = null;
        this.mergedCreatedByString = null;
        this.mergedKeyValueMetaData = new HashMap<>();
        this.mergedBlocks = new ArrayList<>();
        this.mergedColumnTypes = null;
        this.mergedVersion = null;
    }

    /**
     * Added parquet metadata for provided parquet file.
     *
     * @param parquetFile The parquet file destination path
     * @param metadata The parquet metadata
     */
    public void addParquetFileMetadata(final File parquetFile, final ParquetMetadata metadata) {
        parquetFileMetadataList.add(new ParquetFileMetadata(parquetFile, metadata));
    }

    /**
     * Write the accumulated metadata to the provided files and clear the metadata accumulated so far.
     *
     * @param metadataFile The destination for the {@value ParquetUtils#METADATA_FILE_NAME} file
     * @param commonMetadataFile The destination for the {@value ParquetUtils#COMMON_METADATA_FILE_NAME} file
     */
    public void writeMetadataFiles(final File metadataFile, final File commonMetadataFile) throws IOException {
        if (parquetFileMetadataList.isEmpty()) {
            throw new UncheckedDeephavenException("No parquet files to write metadata for");
        }
        mergeMetadata();
        final ParquetMetadata metadataFooter = new ParquetMetadata(new FileMetaData(mergedSchema,
                mergedKeyValueMetaData, mergedCreatedByString), mergedBlocks);
        writeMetadataFile(metadataFooter, metadataFile.getAbsolutePath());

        // Skip the blocks data and merge schema with partitioning columns' schema to write the common metadata file.
        // The ordering of arguments in method call is important because we want to keep partitioning columns in the
        // beginning.
        mergedSchema = mergeSchemaInto(mergedSchema, partitioningColumnsSchema);
        final ParquetMetadata commonMetadataFooter =
                new ParquetMetadata(new FileMetaData(mergedSchema, mergedKeyValueMetaData, mergedCreatedByString),
                        new ArrayList<>());
        writeMetadataFile(commonMetadataFooter, commonMetadataFile.toString());

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
            mergeKeyValueMetaData(fileMetaData.getKeyValueMetaData());
            mergeBlocksInto(parquetFileMetadata, metadataRootDirAbsPath, mergedBlocks);
            mergedCreatedBy.add(fileMetaData.getCreatedBy());
        }
        // Generate the merged deephaven-specific metadata
        final TableInfo.Builder tableInfoBuilder = TableInfo.builder().addAllColumnTypes(mergedColumnTypes);
        if (mergedVersion != null) {
            tableInfoBuilder.version(mergedVersion);
        }
        mergedKeyValueMetaData.put(ParquetTableWriter.METADATA_KEY, tableInfoBuilder.build().serializeToJSON());
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
     * Merge the non-deephaven-specific key-value metadata for a file into the merged metadata. For deephaven specific
     * fields, we filter the relevant fields here and merge them later once all the files have been processed.
     */
    private void mergeKeyValueMetaData(final Map<String, String> keyValueMetaData) throws IOException {
        for (final Map.Entry<String, String> entry : keyValueMetaData.entrySet()) {
            if (!entry.getKey().equals(ParquetTableWriter.METADATA_KEY)) {
                // We should only have one value for each key
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
                // For merging deephaven-specific metadata,
                // - groupingColumns, dataIndexes are skipped
                // - columnTypes must be the same for all partitions
                // - version is set as non-null if all the files have the same version
                final TableInfo tableInfo = TableInfo.deserializeFromJSON(entry.getValue());
                if (mergedColumnTypes == null) {
                    // First time we've seen deephaven specific metadata, so just copy the relevant fields
                    mergedColumnTypes = tableInfo.columnTypes();
                    mergedVersion = tableInfo.version();
                } else {
                    if (!mergedColumnTypes.equals(tableInfo.columnTypes())) {
                        throw new UncheckedDeephavenException("Could not merge metadata for key " +
                                ParquetTableWriter.METADATA_KEY + ", has conflicting values for columnTypes: " +
                                tableInfo.columnTypes() + " and " + mergedColumnTypes);
                    }
                    if (!tableInfo.version().equals(mergedVersion)) {
                        mergedVersion = null;
                    }
                }
            }
        }
    }

    private static void mergeBlocksInto(final ParquetFileMetadata parquetFileMetadata,
            final Path metadataRootDirAbsPath, final Collection<BlockMetaData> mergedBlocks) {
        final Path parquetFileAbsPath = Paths.get(parquetFileMetadata.file.getAbsolutePath());
        String fileRelativePathString = metadataRootDirAbsPath.relativize(parquetFileAbsPath).toString();
        // Remove leading slashes from the relative path
        int pos = 0;
        while (pos < fileRelativePathString.length() && fileRelativePathString.charAt(pos) == '/') {
            pos++;
        }
        fileRelativePathString = fileRelativePathString.substring(pos);
        for (final BlockMetaData block : parquetFileMetadata.metadata.getBlocks()) {
            block.setPath(fileRelativePathString);
            mergedBlocks.add(block);
        }
    }

    private void writeMetadataFile(final ParquetMetadata metadataFooter, final String outputPath) throws IOException {
        final PositionedBufferedOutputStream metadataOutputStream =
                new PositionedBufferedOutputStream(channelsProvider.getWriteChannel(outputPath, false),
                        ParquetUtils.PARQUET_OUTPUT_BUFFER_SIZE);
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
