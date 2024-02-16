package io.deephaven.parquet.table;

import io.deephaven.UncheckedDeephavenException;
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.deephaven.util.channel.SeekableChannelsProvider.convertToURI;

/**
 * Used to generate a combined _metadata and _common_metadata file for provided Parquet files.
 */
public final class ParquetMetadataFileWriterImpl implements ParquetMetadataFileWriter {

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

    /**
     * A class to accumulate metadata from multiple parquet files.
     */
    private static final class AccumulatedMetadata {
        private MessageType schema;
        private final Map<String, String> keyValueMetaData;
        private final Set<String> createdBy;
        private List<ColumnTypeInfo> columnTypes;

        AccumulatedMetadata() {
            this.schema = null;
            this.keyValueMetaData = new HashMap<>();
            this.createdBy = new HashSet<>();
            this.columnTypes = null;
        }

        void combineWith(final FileMetaData fileMetaData) throws IOException {
            // Take union of the schema
            if (schema == null) {
                schema = fileMetaData.getSchema();
            } else if (!schema.equals(fileMetaData.getSchema())) {
                schema = schema.union(fileMetaData.getSchema(), true);
            }

            // Accumulate the createdBy metadata
            createdBy.add(fileMetaData.getCreatedBy());

            // Accumulate the key-value metadata
            for (final Map.Entry<String, String> entry : fileMetaData.getKeyValueMetaData().entrySet()) {
                if (!entry.getKey().equals(ParquetTableWriter.METADATA_KEY)) {
                    // We should only have one value for each key
                    if (!keyValueMetaData.containsKey(entry.getKey())) {
                        keyValueMetaData.put(entry.getKey(), entry.getValue());
                    } else if (!keyValueMetaData.get(entry.getKey()).equals(entry.getValue())) {
                        throw new UncheckedDeephavenException("Could not merge metadata for key " + entry.getKey() +
                                ", has conflicting values: " + entry.getValue() + " and "
                                + keyValueMetaData.get(entry.getKey()));
                    }
                } else {
                    // For merging deephaven-specific metadata,
                    // - groupingColumns, dataIndexes should always be dropped
                    // - version is optional, so we read it from the first file's metadata
                    // - columnTypes must be the same for all partitions
                    final TableInfo tableInfo = TableInfo.deserializeFromJSON(entry.getValue());
                    if (!keyValueMetaData.containsKey(ParquetTableWriter.METADATA_KEY)) {
                        columnTypes = tableInfo.columnTypes();
                        keyValueMetaData.put(ParquetTableWriter.METADATA_KEY,
                                TableInfo.builder().addAllColumnTypes(columnTypes)
                                        .version(tableInfo.version())
                                        .build().serializeToJSON());
                    } else if (!columnTypes.equals(tableInfo.columnTypes())) {
                        throw new UncheckedDeephavenException("Could not merge metadata for key " +
                                ParquetTableWriter.METADATA_KEY + ", has conflicting values for columnTypes: " +
                                entry.getValue() + " and " + keyValueMetaData.get(entry.getKey()));
                    }
                }
            }
        }

        FileMetaData getMerged() {
            final String createdByString = createdBy.size() == 1 ? createdBy.iterator().next() : createdBy.toString();
            return new FileMetaData(schema, keyValueMetaData, createdByString);
        }
    }

    private final String metadataRootDirAbsPath;
    private final List<ParquetFileMetadata> parquetFileMetadataList;
    private final SeekableChannelsProvider channelsProvider;

    public ParquetMetadataFileWriterImpl(final String metadataRootDir, final File[] destinations) {
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

    public void writeMetadataFiles(final File metadataFile, final File commonMetadataFile) throws IOException {
        final ParquetMetadata metadataFooter = mergeMetadata();
        writeMetadataFile(metadataFooter, metadataFile.getAbsolutePath());

        metadataFooter.getBlocks().clear();
        writeMetadataFile(metadataFooter, commonMetadataFile.toString());
        parquetFileMetadataList.clear();
    }

    private ParquetMetadata mergeMetadata() throws IOException {
        final AccumulatedMetadata accumulatedMetadata = new AccumulatedMetadata();
        final List<BlockMetaData> blocks = new ArrayList<>();
        for (final ParquetFileMetadata parquetFileMetadata : parquetFileMetadataList) {
            accumulatedMetadata.combineWith(parquetFileMetadata.metadata.getFileMetaData());
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
                blocks.add(block);
            }
        }
        return new ParquetMetadata(accumulatedMetadata.getMerged(), blocks);
    }

    private void writeMetadataFile(final ParquetMetadata metadataFooter, final String outputPath) throws IOException {
        final PositionedBufferedOutputStream metadataOutputStream =
                new PositionedBufferedOutputStream(channelsProvider.getWriteChannel(outputPath, false));
        metadataOutputStream.write(ParquetFileReader.MAGIC);
        ParquetFileWriter.serializeFooter(metadataFooter, metadataOutputStream);
        metadataOutputStream.close();
    }
}
