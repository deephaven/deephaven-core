//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class RowGroupReaderImpl implements RowGroupReader {
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
    @Nullable
    public ColumnChunkReaderImpl getColumnChunk(@NotNull final String columnName, @NotNull final List<String> path) {
        final String key = path.toString();
        final ColumnChunk columnChunk = chunkMap.get(key);
        final List<Type> fieldTypes = schemaMap.get(key);
        if (columnChunk == null) {
            return null;
        }
        return new ColumnChunkReaderImpl(columnName, columnChunk, channelsProvider, rootURI, type, fieldTypes,
                numRows(), version);
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
