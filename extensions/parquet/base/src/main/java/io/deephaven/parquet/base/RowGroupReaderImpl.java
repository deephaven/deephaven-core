//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

final class RowGroupReaderImpl implements RowGroupReader {
    private final RowGroup rowGroup;
    private final SeekableChannelsProvider channelsProvider;
    private final MessageType schema;
    private final Map<String, List<Type>> schemaMap;
    private final Map<String, ColumnChunk> chunkMap;
    private final Map<Integer, List<Type>> schemaMapByFieldId;
    private final Map<Integer, ColumnChunk> chunkMapByFieldId;

    /**
     * If reading a single parquet file, root URI is the URI of the file, else the parent directory for a metadata file
     */
    private final URI rootURI;
    private final String version;

    RowGroupReaderImpl(
            @NotNull final RowGroup rowGroup,
            @NotNull final SeekableChannelsProvider channelsProvider,
            @NotNull final URI rootURI,
            @NotNull final MessageType schema,
            @Nullable final String version) {
        final int fieldCount = schema.getFieldCount();
        if (rowGroup.getColumnsSize() != fieldCount) {
            throw new IllegalStateException(String.format(
                    "Expected schema columnsCount and row group columns size to be equal, schema.getFieldCount()=%d, rowGroup.getColumnsSize()=%d, rootURI=%s",
                    fieldCount, rowGroup.getColumnsSize(), rootURI));
        }
        this.channelsProvider = Objects.requireNonNull(channelsProvider);
        this.rowGroup = Objects.requireNonNull(rowGroup);
        this.rootURI = Objects.requireNonNull(rootURI);
        this.schema = Objects.requireNonNull(schema);
        schemaMap = new HashMap<>(fieldCount);
        chunkMap = new HashMap<>(fieldCount);
        schemaMapByFieldId = new HashMap<>(fieldCount);
        chunkMapByFieldId = new HashMap<>(fieldCount);
        // Note: there is no technical guarantee from parquet that column names, path_in_schema, or field_ids are
        // unique; it's technically possible that they are duplicated. Ultimately, getColumnChunk is a bad abstraction -
        // we shouldn't need to re-do matching for every single row group column chunk, the matching should be done
        // _once_ per column to get the column index, and then for every row group we should just need to do
        // rowGroup.getColumns().get(columnIndex).
        //
        // Also, this logic divorced from our inference
        // (io.deephaven.parquet.table.ParquetSchemaReader.readParquetSchema)
        // makes it harder to keep the two in-sync.
        final Set<String> nonUniqueKeys = new HashSet<>();
        final Set<Integer> nonUniqueFieldIds = new HashSet<>();
        final Iterator<Type> fieldsIt = schema.getFields().iterator();
        final Iterator<ColumnChunk> colsIt = rowGroup.getColumnsIterator();
        while (fieldsIt.hasNext() && colsIt.hasNext()) {
            final Type ft = fieldsIt.next();
            final ColumnChunk column = colsIt.next();
            final List<String> path_in_schema = column.getMeta_data().path_in_schema;
            final String key = path_in_schema.toString();
            final List<Type> nonRequiredFields = new ArrayList<>();
            for (int indexInPath = 0; indexInPath < path_in_schema.size(); indexInPath++) {
                Type fieldType = schema
                        .getType(path_in_schema.subList(0, indexInPath + 1).toArray(new String[0]));
                if (fieldType.getRepetition() != Repetition.REQUIRED) {
                    nonRequiredFields.add(fieldType);
                }
            }
            if (chunkMap.putIfAbsent(key, column) != null) {
                nonUniqueKeys.add(key);
            }
            schemaMap.putIfAbsent(key, nonRequiredFields);
            if (ft.getId() != null) {
                final int fieldId = ft.getId().intValue();
                if (chunkMapByFieldId.putIfAbsent(fieldId, column) != null) {
                    nonUniqueFieldIds.add(fieldId);
                }
                schemaMapByFieldId.putIfAbsent(fieldId, nonRequiredFields);
            }
        }
        if (fieldsIt.hasNext() || colsIt.hasNext()) {
            throw new IllegalStateException(String.format("Unexpected, iterators not exhausted, rootURI=%s", rootURI));
        }
        for (String nonUniqueKey : nonUniqueKeys) {
            chunkMap.remove(nonUniqueKey);
            schemaMap.remove(nonUniqueKey);
        }
        for (Integer nonUniqueFieldId : nonUniqueFieldIds) {
            chunkMapByFieldId.remove(nonUniqueFieldId);
            schemaMapByFieldId.remove(nonUniqueFieldId);
        }
        this.version = version;
    }

    @Override
    @Nullable
    public ColumnChunkReaderImpl getColumnChunk(@NotNull final String columnName, @NotNull final List<String> path,
            @Nullable Integer fieldId) {
        final ColumnChunk columnChunk;
        final List<Type> nonRequiredFields;
        PARTS: {
            if (fieldId != null) {
                final ColumnChunk cc = chunkMapByFieldId.get(fieldId);
                if (cc != null) {
                    columnChunk = cc;
                    nonRequiredFields = schemaMapByFieldId.get(fieldId);
                    break PARTS;
                }
            }
            final String key = path.toString();
            final ColumnChunk cc = chunkMap.get(key);
            if (cc == null) {
                return null;
            }
            columnChunk = cc;
            nonRequiredFields = schemaMap.get(key);
        }
        return new ColumnChunkReaderImpl(columnName, columnChunk, channelsProvider, rootURI, schema, nonRequiredFields,
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
