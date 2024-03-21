//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelContext.ContextHolder;
import io.deephaven.util.channel.SeekableChannelContext;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class RowGroupReaderImpl implements RowGroupReader {

    private static final int BUFFER_SIZE = 65536;
    private final RowGroup rowGroup;
    private final SeekableChannelsProvider channelsProvider;
    private final MessageType type;
    private final Map<String, List<Type>> schemaMap = new HashMap<>();
    private final Map<String, ColumnChunk> chunkMap = new HashMap<>();

    /**
     * If reading a single parquet file, root URI is the URI of the file, else the parent directory for a metadata file
     */
    private final URI rootURI;
    private final String version;

    RowGroupReaderImpl(
            @NotNull final RowGroup rowGroup,
            @NotNull final SeekableChannelsProvider channelsProvider,
            @NotNull final URI rootURI,
            @NotNull final MessageType type,
            @NotNull final MessageType schema,
            @Nullable final String version) {
        this.channelsProvider = channelsProvider;
        this.rowGroup = rowGroup;
        this.rootURI = rootURI;
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
        this.version = version;
    }

    @Override
    public ColumnChunkReaderImpl getColumnChunk(@NotNull final List<String> path,
            @NotNull final SeekableChannelContext channelContext) {
        String key = path.toString();
        ColumnChunk columnChunk = chunkMap.get(key);
        List<Type> fieldTypes = schemaMap.get(key);
        if (columnChunk == null) {
            return null;
        }
        final OffsetIndex offsetIndex = offsetIndex(columnChunk, channelContext);
        return new ColumnChunkReaderImpl(columnChunk, channelsProvider, rootURI, type, offsetIndex, fieldTypes,
                numRows(), version);
    }

    private OffsetIndex offsetIndex(ColumnChunk chunk, @NotNull SeekableChannelContext context) {
        if (!chunk.isSetOffset_index_offset()) {
            return null;
        }
        return ParquetMetadataConverter.fromParquetOffsetIndex(readOffsetIndex(chunk, context));
    }

    private org.apache.parquet.format.OffsetIndex readOffsetIndex(ColumnChunk chunk,
            @NotNull SeekableChannelContext channelContext) {
        try (
                final ContextHolder holder = SeekableChannelContext.ensureContext(channelsProvider, channelContext);
                final SeekableByteChannel readChannel = channelsProvider.getReadChannel(holder.get(), rootURI);
                final InputStream in =
                        channelsProvider.getInputStream(readChannel.position(chunk.getOffset_index_offset()))) {
            return Util.readOffsetIndex(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
