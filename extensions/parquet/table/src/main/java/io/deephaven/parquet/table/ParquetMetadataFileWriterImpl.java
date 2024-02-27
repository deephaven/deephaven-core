package io.deephaven.parquet.table;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.parquet.base.ParquetFileReader;
import io.deephaven.parquet.base.ParquetFileWriter;
import io.deephaven.parquet.base.ParquetMetadataFileWriter;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static io.deephaven.util.channel.SeekableChannelsProvider.convertToURI;

/**
 * Used to generate a combined _metadata and _common_metadata file for provided Parquet files.
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

    private final String metadataRootDirAbsPath;
    private final List<ParquetFileMetadata> parquetFileMetadataList;
    private final SeekableChannelsProvider channelsProvider;
    private final MessageType commonSchema;

    // The following fields are used to accumulate metadata for all parquet files
    private MessageType mergedSchema;
    private String mergedCreatedByString;
    private final Map<String, String> mergedKeyValueMetaData;
    private final List<BlockMetaData> mergedBlocks;

    /**
     * Per-column type information stored in key-value metadata
     */
    private List<ColumnTypeInfo> columnTypes;

    /**
     * @param metadataRootDir The root directory for the metadata files
     * @param destinations The indivdual parquet file destinations, all of which must be contained in the metadata root
     * @param commonSchema The common schema to be included for writing the _common_metadata file.
     */
    ParquetMetadataFileWriterImpl(@NotNull final String metadataRootDir, @NotNull final File[] destinations,
            @Nullable final MessageType commonSchema) {
        for (final File destination : destinations) {
            if (!destination.getAbsolutePath().startsWith(metadataRootDir)) {
                throw new UncheckedDeephavenException("All destinations must be contained in the provided metadata root"
                        + " directory, provided destination " + destination.getAbsolutePath() + " is not in " +
                        metadataRootDir);
            }
        }
        final File rootDir = new File(metadataRootDir);
        this.metadataRootDirAbsPath = rootDir.getAbsolutePath();
        this.parquetFileMetadataList = new ArrayList<>(destinations.length);
        this.channelsProvider =
                SeekableChannelsProviderLoader.getInstance().fromServiceLoader(convertToURI(rootDir.getPath()), null);
        this.columnTypes = null;
        this.commonSchema = commonSchema;

        this.mergedSchema = null;
        this.mergedCreatedByString = null;
        this.mergedKeyValueMetaData = new HashMap<>();
        this.mergedBlocks = new ArrayList<>();
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
     * @param metadataFile The destination file for the _metadata file
     * @param commonMetadataFile The destination file for the _common_metadata file
     */
    public void writeMetadataFiles(final File metadataFile, final File commonMetadataFile) throws IOException {
        if (parquetFileMetadataList.isEmpty()) {
            throw new UncheckedDeephavenException("No parquet files to write metadata for");
        }
        mergeMetadata();
        final ParquetMetadata metadataFooter =
                new ParquetMetadata(new FileMetaData(mergedSchema, mergedKeyValueMetaData, mergedCreatedByString),
                        mergedBlocks);
        writeMetadataFile(metadataFooter, metadataFile.getAbsolutePath());

        // Skip the blocks data and merge schema with the common schema to write the common metadata file
        // The ordering of arguments in method call is important because we want common schema to determine the overall
        // ordering of the schema fields.
        mergedSchema = mergeSchemaInto(mergedSchema, commonSchema);
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
            mergeKeyValueMetaDataInto(fileMetaData.getKeyValueMetaData(), mergedKeyValueMetaData);
            mergeBlocksInto(parquetFileMetadata, metadataRootDirAbsPath, mergedBlocks);
            mergedCreatedBy.add(fileMetaData.getCreatedBy());
        }
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

    private void mergeKeyValueMetaDataInto(final Map<String, String> keyValueMetaData,
            final Map<String, String> mergedKeyValueMetaData) throws IOException {
        for (final Map.Entry<String, String> entry : keyValueMetaData.entrySet()) {
            if (!entry.getKey().equals(ParquetTableWriter.METADATA_KEY)) {
                // We should only have one value for each key
                if (!mergedKeyValueMetaData.containsKey(entry.getKey())) {
                    mergedKeyValueMetaData.put(entry.getKey(), entry.getValue());
                } else if (!mergedKeyValueMetaData.get(entry.getKey()).equals(entry.getValue())) {
                    throw new UncheckedDeephavenException("Could not merge metadata for key " + entry.getKey() +
                            ", has conflicting values: " + entry.getValue() + " and "
                            + mergedKeyValueMetaData.get(entry.getKey()));
                }
            } else {
                // For merging deephaven-specific metadata,
                // - groupingColumns, dataIndexes are skipped
                // - version is optional, so we read it from the first file's metadata
                // - columnTypes must be the same for all partitions
                final TableInfo tableInfo = TableInfo.deserializeFromJSON(entry.getValue());
                if (!mergedKeyValueMetaData.containsKey(ParquetTableWriter.METADATA_KEY)) {
                    // First time we've seen deephaven specific metadata
                    Assert.eqNull(columnTypes, "columnTypes");
                    columnTypes = tableInfo.columnTypes();
                    mergedKeyValueMetaData.put(ParquetTableWriter.METADATA_KEY,
                            TableInfo.builder()
                                    .addAllColumnTypes(columnTypes)
                                    .version(tableInfo.version())
                                    .build()
                                    .serializeToJSON());
                } else if (!columnTypes.equals(tableInfo.columnTypes())) {
                    throw new UncheckedDeephavenException("Could not merge metadata for key " +
                            ParquetTableWriter.METADATA_KEY + ", has conflicting values for columnTypes: " +
                            entry.getValue() + " and " + mergedKeyValueMetaData.get(entry.getKey()));
                }
            }
        }
    }

    private static void mergeBlocksInto(final ParquetFileMetadata parquetFileMetadata,
            final String metadataRootDirAbsPath,
            final List<BlockMetaData> mergedBlocks) {
        final String fileAbsolutePath = parquetFileMetadata.file.getAbsolutePath();
        String fileRelativePath = fileAbsolutePath.substring(metadataRootDirAbsPath.length());
        // Remove leading slashes from the relative path
        int pos = 0;
        while (pos < fileRelativePath.length() && fileRelativePath.charAt(pos) == '/') {
            pos++;
        }
        fileRelativePath = fileRelativePath.substring(pos);
        for (final BlockMetaData block : parquetFileMetadata.metadata.getBlocks()) {
            block.setPath(fileRelativePath);
            mergedBlocks.add(block);
        }
    }

    private void writeMetadataFile(final ParquetMetadata metadataFooter, final String outputPath) throws IOException {
        final PositionedBufferedOutputStream metadataOutputStream =
                new PositionedBufferedOutputStream(channelsProvider.getWriteChannel(outputPath, false));
        metadataOutputStream.write(ParquetFileReader.MAGIC);
        ParquetFileWriter.serializeFooter(metadataFooter, metadataOutputStream);
        metadataOutputStream.close();
    }

    public void clear() {
        parquetFileMetadataList.clear();
        mergedKeyValueMetaData.clear();
        mergedBlocks.clear();
        columnTypes = null;
        mergedSchema = null;
        mergedCreatedByString = null;
    }
}
