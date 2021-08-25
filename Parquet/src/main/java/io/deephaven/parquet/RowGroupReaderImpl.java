package io.deephaven.parquet;

import io.deephaven.parquet.utils.SeekableChannelsProvider;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowGroupReaderImpl implements RowGroupReader {

    private static final int BUFFER_SIZE = 65536;
    private final RowGroup rowGroup;
    private final SeekableChannelsProvider channelsProvider;
    private final ThreadLocal<CodecFactory> codecFactory;
    private final MessageType type;
    private final Map<String, List<Type>> schemaMap = new HashMap<>();
    private Map<String, ColumnChunk> chunkMap = new HashMap<>();

    private final Path rootPath;

    RowGroupReaderImpl(RowGroup rowGroup, SeekableChannelsProvider channelsProvider, Path rootPath,
            ThreadLocal<CodecFactory> codecFactory, MessageType type, MessageType schema) {
        this.channelsProvider = channelsProvider;
        this.codecFactory = codecFactory;
        this.rowGroup = rowGroup;
        this.rootPath = rootPath;
        this.type = type;
        for (ColumnChunk column : rowGroup.columns) {
            List<String> path_in_schema = column.getMeta_data().path_in_schema;
            String key = path_in_schema.toString();
            chunkMap.put(key, column);
            List<Type> nonRequiredFields = new ArrayList<>();
            for (int indexInPath = 0; indexInPath < path_in_schema.size(); indexInPath++) {
                Type fieldType = schema
                        .getType(path_in_schema.subList(0, indexInPath + 1).toArray(new String[0]));
                if (fieldType.getRepetition() != Type.Repetition.REQUIRED) {
                    nonRequiredFields.add(fieldType);
                }
            }
            schemaMap.put(key, nonRequiredFields);
        }
    }

    @Override
    public ColumnChunkReaderImpl getColumnChunk(List<String> path) {
        String key = path.toString();
        ColumnChunk columnChunk = chunkMap.get(key);
        List<Type> fieldTypes = schemaMap.get(key);
        if (columnChunk == null) {
            return null;
        }

        OffsetIndex offsetIndex = null;

        if (columnChunk == null) {
            return null;
        }
        if (columnChunk.isSetOffset_index_offset()) {
            try (final SeekableByteChannel readChannel = channelsProvider.getReadChannel(rootPath)) {
                readChannel.position(columnChunk.getOffset_index_offset());
                offsetIndex = ParquetMetadataConverter.fromParquetOffsetIndex(Util.readOffsetIndex(
                        new BufferedInputStream(Channels.newInputStream(readChannel), BUFFER_SIZE)));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return new ColumnChunkReaderImpl(columnChunk, channelsProvider, rootPath, codecFactory,
                type, offsetIndex, fieldTypes);
    }

    @Override
    public long numRows() {
        return rowGroup.num_rows;
    }

    @Override
    public RowGroup getRowGroup() {
        return rowGroup;
    }
}
