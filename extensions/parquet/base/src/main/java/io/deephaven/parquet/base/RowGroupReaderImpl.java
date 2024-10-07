//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.ID;
import org.apache.parquet.schema.Type.Repetition;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

final class RowGroupReaderImpl implements RowGroupReader {

    private class ColumnHolder {
        private final int fieldIndex;
        private final int columnIndex;

        public ColumnHolder(int fieldIndex, int columnIndex) {
            this.fieldIndex = fieldIndex;
            this.columnIndex = columnIndex;
        }

        String pathKey() {
            return pathInSchema().toString();
        }

        Integer fieldId() {
            final ID id = fieldType().getId();
            return id == null ? null : id.intValue();
        }

        ColumnChunkReaderImpl reader(String columnName) {
            return new ColumnChunkReaderImpl(columnName, columnChunk(), channelsProvider, rootURI, schema,
                    nonRequiredFields(), numRows(), version);
        }

        private Type fieldType() {
            return schema.getFields().get(fieldIndex);
        }

        private List<String> pathInSchema() {
            return columnChunk().getMeta_data().path_in_schema;
        }

        private ColumnChunk columnChunk() {
            return rowGroup.getColumns().get(columnIndex);
        }

        private List<Type> nonRequiredFields() {
            final List<String> path_in_schema = pathInSchema();
            final List<Type> nonRequiredFields = new ArrayList<>();
            for (int indexInPath = 0; indexInPath < path_in_schema.size(); indexInPath++) {
                Type fieldType = schema
                        .getType(path_in_schema.subList(0, indexInPath + 1).toArray(new String[0]));
                if (fieldType.getRepetition() != Repetition.REQUIRED) {
                    nonRequiredFields.add(fieldType);
                }
            }
            return nonRequiredFields;
        }

        private String debugString() {
            return String.format("colIx=%d, pathKey=%s, fieldId=%d", columnIndex, pathKey(), fieldId());
        }
    }

    private final RowGroup rowGroup;
    private final SeekableChannelsProvider channelsProvider;
    private final MessageType schema;
    private final Map<String, ColumnHolder> byPath;
    private final Map<Integer, ColumnHolder> byFieldId;

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
        this.channelsProvider = Objects.requireNonNull(channelsProvider);
        this.rowGroup = Objects.requireNonNull(rowGroup);
        this.rootURI = Objects.requireNonNull(rootURI);
        this.schema = Objects.requireNonNull(schema);
        byPath = new HashMap<>(fieldCount);
        byFieldId = new HashMap<>(fieldCount);
        // Note: there is no technical guarantee from parquet that column names, path_in_schema, or field_ids are
        // unique; it's technically possible that they are duplicated. Ultimately, getColumnChunk is a bad abstraction -
        // we shouldn't need to re-do matching for every single row group column chunk, the matching should be done
        // _once_ per column to get the column index, and then for every row group we should just need to do
        // rowGroup.getColumns().get(columnIx).
        //
        // Also, this logic divorced from our inference
        // (io.deephaven.parquet.table.ParquetSchemaReader.readParquetSchema)
        // makes it harder to keep the two in-sync.
        final Set<String> nonUniquePaths = new HashSet<>();
        final Set<Integer> nonUniqueFieldIds = new HashSet<>();
        int fieldIx = 0;
        int columnIx = 0;
        for (final Type fieldType : schema.getFields()) {
            final int totalColumns = ParquetTotalColumns.of(fieldType);
            if (totalColumns == 1) {
                final ColumnHolder holder = new ColumnHolder(fieldIx, columnIx);
                final String key = holder.pathKey();
                final Integer fieldId = holder.fieldId();
                if (byPath.putIfAbsent(key, holder) != null) {
                    nonUniquePaths.add(key);
                }
                if (fieldId != null) {
                    if (byFieldId.putIfAbsent(fieldId, holder) != null) {
                        nonUniqueFieldIds.add(fieldId);
                    }
                }
            }
            columnIx += totalColumns;
            ++fieldIx;
        }
        if (columnIx != schema.getPaths().size()) {
            throw new IllegalStateException(
                    String.format("Inconsistent column count, columnIx=%d, schema.getPaths().size()=%d", columnIx,
                            schema.getPaths().size()));
        }
        for (String nonUniquePath : nonUniquePaths) {
            byPath.remove(nonUniquePath);
        }
        for (Integer nonUniqueFieldId : nonUniqueFieldIds) {
            byFieldId.remove(nonUniqueFieldId);
        }
        this.version = version;
    }

    @Override
    public @Nullable ColumnChunkReader getColumnChunk(
            @NotNull String columnName,
            @NotNull List<String> defaultPath,
            @Nullable List<String> parquetColumnNamePath,
            @Nullable Integer fieldId) {
        final ColumnHolder holder;
        if (fieldId == null && parquetColumnNamePath == null) {
            holder = byPath.get(defaultPath.toString());
        } else {
            final ColumnHolder byFieldId = fieldId == null ? null : this.byFieldId.get(fieldId);
            final ColumnHolder byPath =
                    parquetColumnNamePath == null ? null : this.byPath.get(parquetColumnNamePath.toString());
            if (byFieldId != null && byPath != null) {
                if (byFieldId != byPath) {
                    throw new IllegalArgumentException(String.format(
                            "For columnName=%s, providing an explicit parquet column name path (%s) and field id (%d) mapping, but they are resolving to different columns, byFieldId=[%s], byPath=[%s]",
                            columnName, parquetColumnNamePath, fieldId, byFieldId.debugString(), byPath.debugString()));
                }
            }
            holder = byFieldId != null ? byFieldId : byPath;
        }
        return holder == null ? null : holder.reader(columnName);
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
