/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.InternalFileEncryptor;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.parquet.compress.CompressorAdapter;
import io.deephaven.parquet.compress.DeephavenCompressorAdapterFactory;
import org.apache.parquet.Version;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesUtils;

import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.parquet.format.Util.writeFileMetaData;
import static org.apache.parquet.hadoop.ParquetFileWriter.CURRENT_VERSION;
import static org.apache.parquet.hadoop.ParquetFileWriter.MAGIC;

public final class ParquetFileWriter {
    private static final ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter();
    private static final int VERSION = 1;
    private static final int OUTPUT_BUFFER_SIZE = 1 << 18;

    private final PositionedBufferedOutputStream bufferedOutput;
    private final MessageType type;
    private final int targetPageSize;
    private final ByteBufferAllocator allocator;
    private final CompressorAdapter compressorAdapter;
    private final Map<String, String> extraMetaData;
    private final List<BlockMetaData> blocks = new ArrayList<>();
    private final List<List<OffsetIndex>> offsetIndexes = new ArrayList<>();
    private final String filePath;
    private final SeekableChannelsProvider channelsProvider;
    private static final String PARQUET_METADATA_FILE = "_metadata";
    private static final String PARQUET_COMMON_METADATA_FILE = "_common_metadata";

    public ParquetFileWriter(
            final String filePath,
            final SeekableChannelsProvider channelsProvider,
            final int targetPageSize,
            final ByteBufferAllocator allocator,
            final MessageType type,
            final String codecName,
            final Map<String, String> extraMetaData) throws IOException {
        this.targetPageSize = targetPageSize;
        this.allocator = allocator;
        this.extraMetaData = new HashMap<>(extraMetaData);
        bufferedOutput = new PositionedBufferedOutputStream(channelsProvider.getWriteChannel(filePath, false),
                OUTPUT_BUFFER_SIZE);
        bufferedOutput.write(ParquetFileReader.MAGIC);
        this.type = type;
        this.compressorAdapter = DeephavenCompressorAdapterFactory.getInstance().getByName(codecName);
        this.filePath = filePath;
        this.channelsProvider = channelsProvider;
    }

    public RowGroupWriter addRowGroup(final long size) {
        final RowGroupWriterImpl rowGroupWriter =
                new RowGroupWriterImpl(bufferedOutput, type, targetPageSize, allocator, compressorAdapter);
        rowGroupWriter.getBlock().setRowCount(size);
        blocks.add(rowGroupWriter.getBlock());
        offsetIndexes.add(rowGroupWriter.offsetIndexes());
        return rowGroupWriter;
    }

    public void close() throws IOException {
        serializeOffsetIndexes();
        final ParquetMetadata footer =
                new ParquetMetadata(new FileMetaData(type, extraMetaData, Version.FULL_VERSION), blocks);
        serializeFooter(footer);
        // Flush any buffered data and close the channel
        bufferedOutput.close();
        compressorAdapter.close();
        // Write the metadata file
        final Path metadataDirPath = new Path(new File(filePath).getParent());
        final String actualFilePath = filePath.replace(".NEW_", "");
        final Footer fsFooter = new Footer(new Path(actualFilePath), footer);
        final ArrayList<Footer> footers = new ArrayList<>();
        footers.add(fsFooter);
        writeMetadataFile(metadataDirPath, footers);
    }

    @Deprecated
    public void writeMetadataFile(Path outputPath, List<Footer> footers) throws IOException {
        ParquetMetadata metadataFooter = mergeFooters(outputPath, footers);

        writeMetadataFile(outputPath, metadataFooter, PARQUET_METADATA_FILE);

        metadataFooter.getBlocks().clear();
        writeMetadataFile(outputPath, metadataFooter, PARQUET_COMMON_METADATA_FILE);
    }

    static GlobalMetaData mergeInto(
            FileMetaData toMerge,
            GlobalMetaData mergedMetadata) {
        return mergeInto(toMerge, mergedMetadata, true);
    }

    static GlobalMetaData mergeInto(
            FileMetaData toMerge,
            GlobalMetaData mergedMetadata,
            boolean strict) {
        MessageType schema = null;
        Map<String, Set<String>> newKeyValues = new HashMap<String, Set<String>>();
        Set<String> createdBy = new HashSet<String>();
        if (mergedMetadata != null) {
            schema = mergedMetadata.getSchema();
            newKeyValues.putAll(mergedMetadata.getKeyValueMetaData());
            createdBy.addAll(mergedMetadata.getCreatedBy());
        }
        if ((schema == null && toMerge.getSchema() != null)
                || (schema != null && !schema.equals(toMerge.getSchema()))) {
            schema = mergeInto(toMerge.getSchema(), schema, strict);
        }
        for (Map.Entry<String, String> entry : toMerge.getKeyValueMetaData().entrySet()) {
            Set<String> values = newKeyValues.get(entry.getKey());
            if (values == null) {
                values = new LinkedHashSet<String>();
                newKeyValues.put(entry.getKey(), values);
            }
            values.add(entry.getValue());
        }
        createdBy.add(toMerge.getCreatedBy());
        return new GlobalMetaData(
                schema,
                newKeyValues,
                createdBy);
    }

    static MessageType mergeInto(MessageType toMerge, MessageType mergedSchema, boolean strict) {
        if (mergedSchema == null) {
            return toMerge;
        }

        return mergedSchema.union(toMerge, strict);
    }

    static ParquetMetadata mergeFooters(Path root, List<Footer> footers) {
        return mergeFooters(root, footers, new StrictKeyValueMetadataMergeStrategy());
    }

    static ParquetMetadata mergeFooters(Path root, List<Footer> footers,
            KeyValueMetadataMergeStrategy keyValueMergeStrategy) {
        String rootPath = root.toUri().getPath();
        GlobalMetaData fileMetaData = null;
        List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();
        for (Footer footer : footers) {
            String footerPath = footer.getFile().toUri().getPath();
            if (!footerPath.startsWith(rootPath)) {
                throw new ParquetEncodingException(
                        footerPath + " invalid: all the files must be contained in the root " + root);
            }
            footerPath = footerPath.substring(rootPath.length());
            while (footerPath.startsWith("/")) {
                footerPath = footerPath.substring(1);
            }
            fileMetaData = mergeInto(footer.getParquetMetadata().getFileMetaData(), fileMetaData);
            for (BlockMetaData block : footer.getParquetMetadata().getBlocks()) {
                block.setPath(footerPath);
                blocks.add(block);
            }
        }
        return new ParquetMetadata(fileMetaData.merge(keyValueMergeStrategy), blocks);
    }

    /**
     * @deprecated metadata files are not recommended and will be removed in 2.0.0
     */
    @Deprecated
    private void writeMetadataFile(Path outputPathRoot, ParquetMetadata metadataFooter, String parquetMetadataFile)
            throws IOException {
        Path metaDataPath = new Path(outputPathRoot, parquetMetadataFile);
        writeMetadataFile(metaDataPath, metadataFooter);
    }

    /**
     * @deprecated metadata files are not recommended and will be removed in 2.0.0
     */
    @Deprecated
    private void writeMetadataFile(Path outputPath, ParquetMetadata metadataFooter)
            throws IOException {
        final PositionedBufferedOutputStream metadataOutputStream =
                new PositionedBufferedOutputStream(channelsProvider.getWriteChannel(outputPath.toString(), false),
                        OUTPUT_BUFFER_SIZE);
        metadataOutputStream.write(MAGIC);
        serializeMetadataFooter(metadataFooter, metadataOutputStream, null, new ParquetMetadataConverter());
        metadataOutputStream.close();
    }

    private static void serializeMetadataFooter(ParquetMetadata footer, PositionedBufferedOutputStream out,
            InternalFileEncryptor fileEncryptor, ParquetMetadataConverter metadataConverter) throws IOException {

        // Unencrypted file
        if (null == fileEncryptor) {
            long footerIndex = out.position();
            org.apache.parquet.format.FileMetaData parquetMetadata =
                    metadataConverter.toParquetMetadata(CURRENT_VERSION, footer);
            writeFileMetaData(parquetMetadata, out);
            BytesUtils.writeIntLittleEndian(out, (int) (out.position() - footerIndex));
            out.write(MAGIC);
        }
    }


    private void serializeFooter(final ParquetMetadata footer) throws IOException {
        final long footerIndex = bufferedOutput.position();
        final org.apache.parquet.format.FileMetaData parquetMetadata =
                metadataConverter.toParquetMetadata(VERSION, footer);
        writeFileMetaData(parquetMetadata, bufferedOutput);
        BytesUtils.writeIntLittleEndian(bufferedOutput, (int) (bufferedOutput.position() - footerIndex));
        bufferedOutput.write(ParquetFileReader.MAGIC);
    }

    private void serializeOffsetIndexes() throws IOException {
        for (int bIndex = 0, bSize = blocks.size(); bIndex < bSize; ++bIndex) {
            final List<ColumnChunkMetaData> columns = blocks.get(bIndex).getColumns();
            final List<OffsetIndex> blockOffsetIndexes = offsetIndexes.get(bIndex);
            for (int cIndex = 0, cSize = columns.size(); cIndex < cSize; ++cIndex) {
                final OffsetIndex offsetIndex = blockOffsetIndexes.get(cIndex);
                if (offsetIndex == null) {
                    continue;
                }
                final ColumnChunkMetaData column = columns.get(cIndex);
                final long offset = bufferedOutput.position();
                Util.writeOffsetIndex(ParquetMetadataConverter.toParquetOffsetIndex(offsetIndex), bufferedOutput);
                column.setOffsetIndexReference(new IndexReference(offset, (int) (bufferedOutput.position() - offset)));
            }
        }
    }
}
