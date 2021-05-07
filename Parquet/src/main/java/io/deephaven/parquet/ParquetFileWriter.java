package io.deephaven.parquet;

import io.deephaven.parquet.tempfix.ParquetMetadataConverter;
import io.deephaven.parquet.utils.SeekableChannelsProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.Version;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesUtils;

import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.format.Util.writeFileMetaData;

public class ParquetFileWriter {
    private static ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter();
    private static final int VERSION = 1;

    private final SeekableByteChannel writeChannel;
    private final MessageType type;
    private final int pageSize;
    private final ByteBufferAllocator allocator;
    private final SeekableChannelsProvider channelsProvider;
    private final CodecFactory.BytesInputCompressor compressor;
    private final Map<String, String> extraMetaData;
    private List<BlockMetaData> blocks = new ArrayList<>();
    private final List<List<OffsetIndex>> offsetIndexes = new ArrayList<>();

    public ParquetFileWriter(String filePath, SeekableChannelsProvider channelsProvider, int pageSize, ByteBufferAllocator allocator,
                             MessageType type, CompressionCodecName codecName, Map<String, String> extraMetaData) throws IOException {
        this.pageSize = pageSize;
        this.allocator = allocator;
        this.extraMetaData = new HashMap<>(extraMetaData);
        writeChannel = channelsProvider.getWriteChannel(filePath, false);//TODO add support for appending
        this.type = type;
        this.channelsProvider = channelsProvider;
        CodecFactory codecFactory = new CodecFactory(new Configuration(), pageSize);
        this.compressor = codecFactory.getCompressor(codecName);

    }

    RowGroupWriter addRowGroup(String path, boolean append) throws IOException {
        RowGroupWriterImpl rowGroupWriter = new RowGroupWriterImpl(path, append, channelsProvider, type, pageSize, allocator, compressor);
        blocks.add(rowGroupWriter.getBlock());
        return rowGroupWriter;
    }


    public RowGroupWriter addRowGroup(long size) {
        RowGroupWriterImpl rowGroupWriter = new RowGroupWriterImpl(writeChannel, type, pageSize, allocator, compressor);
        rowGroupWriter.getBlock().setRowCount(size);
        blocks.add(rowGroupWriter.getBlock());
        offsetIndexes.add(rowGroupWriter.offsetIndexes());
        return rowGroupWriter;
    }

    public void close() throws IOException {
        serializeOffsetIndexes(offsetIndexes, blocks, writeChannel);
        ParquetMetadata footer = new ParquetMetadata(new FileMetaData(type, extraMetaData, Version.FULL_VERSION), blocks);
        serializeFooter(footer, writeChannel);
        writeChannel.close();

    }

    private static void serializeFooter(ParquetMetadata footer, SeekableByteChannel out) throws IOException {
        long footerIndex = out.position();
        org.apache.parquet.format.FileMetaData parquetMetadata = metadataConverter.toParquetMetadata(VERSION, footer);
        OutputStream os = Channels.newOutputStream(out);
        writeFileMetaData(parquetMetadata, os);
        BytesUtils.writeIntLittleEndian(os, (int) (out.position() - footerIndex));
        os.write(ParquetFileReader.MAGIC);
    }

    private static void serializeOffsetIndexes(
            List<List<OffsetIndex>> offsetIndexes,
            List<BlockMetaData> blocks,
            SeekableByteChannel out) throws IOException {
        for (int bIndex = 0, bSize = blocks.size(); bIndex < bSize; ++bIndex) {
            List<ColumnChunkMetaData> columns = blocks.get(bIndex).getColumns();
            List<OffsetIndex> blockOffsetIndexes = offsetIndexes.get(bIndex);
            for (int cIndex = 0, cSize = columns.size(); cIndex < cSize; ++cIndex) {
                OffsetIndex offsetIndex = blockOffsetIndexes.get(cIndex);
                if (offsetIndex == null) {
                    continue;
                }
                ColumnChunkMetaData column = columns.get(cIndex);
                long offset = out.position();
                Util.writeOffsetIndex(ParquetMetadataConverter.toParquetOffsetIndex(offsetIndex), Channels.newOutputStream(out));
                column.setOffsetIndexReference(new IndexReference(offset, (int) (out.position() - offset)));
            }
        }
    }



}
