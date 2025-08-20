//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.vectorchunkfilter;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.select.ExposesChunkFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterImpl;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A WhereFilter that passes each element of an array or Vector through another wrapped filter. If any element is
 * matched, then the array or vector is considered a match.
 */
public class VectorComponentFilterWrapper extends WhereFilterImpl {
    private final String columnName;
    private final boolean isArray;
    private final Class<?> componentType;
    private final WhereFilter componentFilter;
    ChunkFilter chunkFilter;
    Supplier<VectorChunkFilter> vectorChunkFilterFactory;

    private VectorComponentFilterWrapper(final String columnName,
            final boolean isArray,
            final Class<?> componentType,
            final WhereFilter componentFilter) {
        this.columnName = columnName;
        this.isArray = isArray;
        this.componentType = componentType;
        this.componentFilter = componentFilter;
        if (isArray) {
            if (componentType == char.class) {
                vectorChunkFilterFactory = () -> new CharArrayChunkFilter(this, ChunkFilter.FILTER_CHUNK_SIZE);
            } else if (componentType == byte.class) {
                vectorChunkFilterFactory = () -> new ByteArrayChunkFilter(this, ChunkFilter.FILTER_CHUNK_SIZE);
            } else if (componentType == short.class) {
                vectorChunkFilterFactory = () -> new ShortArrayChunkFilter(this, ChunkFilter.FILTER_CHUNK_SIZE);
            } else if (componentType == int.class) {
                vectorChunkFilterFactory = () -> new IntArrayChunkFilter(this, ChunkFilter.FILTER_CHUNK_SIZE);
            } else if (componentType == long.class) {
                vectorChunkFilterFactory = () -> new LongArrayChunkFilter(this, ChunkFilter.FILTER_CHUNK_SIZE);
            } else if (componentType == float.class) {
                vectorChunkFilterFactory = () -> new FloatArrayChunkFilter(this, ChunkFilter.FILTER_CHUNK_SIZE);
            } else if (componentType == double.class) {
                vectorChunkFilterFactory = () -> new DoubleArrayChunkFilter(this, ChunkFilter.FILTER_CHUNK_SIZE);
            } else {
                vectorChunkFilterFactory = () -> new ObjectArrayChunkFilter(this, ChunkFilter.FILTER_CHUNK_SIZE);
            }
        } else {
            if (componentType == char.class) {
                vectorChunkFilterFactory = () -> new CharVectorChunkFilter(this, ChunkFilter.FILTER_CHUNK_SIZE);
            } else if (componentType == byte.class) {
                vectorChunkFilterFactory = () -> new ByteVectorChunkFilter(this, ChunkFilter.FILTER_CHUNK_SIZE);
            } else if (componentType == short.class) {
                vectorChunkFilterFactory = () -> new ShortVectorChunkFilter(this, ChunkFilter.FILTER_CHUNK_SIZE);
            } else if (componentType == int.class) {
                vectorChunkFilterFactory = () -> new IntVectorChunkFilter(this, ChunkFilter.FILTER_CHUNK_SIZE);
            } else if (componentType == long.class) {
                vectorChunkFilterFactory = () -> new LongVectorChunkFilter(this, ChunkFilter.FILTER_CHUNK_SIZE);
            } else if (componentType == float.class) {
                vectorChunkFilterFactory = () -> new FloatVectorChunkFilter(this, ChunkFilter.FILTER_CHUNK_SIZE);
            } else if (componentType == double.class) {
                vectorChunkFilterFactory = () -> new DoubleVectorChunkFilter(this, ChunkFilter.FILTER_CHUNK_SIZE);
            } else {
                vectorChunkFilterFactory = () -> new ObjectVectorChunkFilter(this, ChunkFilter.FILTER_CHUNK_SIZE);
            }
        }
    }

    @Override
    public List<String> getColumns() {
        return List.of(columnName);
    }

    @Override
    public List<String> getColumnArrays() {
        return List.of();
    }

    @Override
    public void init(@NotNull final TableDefinition tableDefinition) {
        componentFilter.init(replaceDefinition(columnName, tableDefinition));

        this.chunkFilter = ((ExposesChunkFilter) componentFilter).chunkFilter()
                .orElseThrow(() -> new UnsupportedOperationException(
                        "Vector component filter not supported for filters that do not implement chunk filter!"));
        if (componentFilter.isRefreshing()) {
            throw new UnsupportedOperationException("Vector component filter not supported for refreshing filters!");
        }
        if (componentFilter.hasVirtualRowVariables()) {
            throw new UnsupportedOperationException(
                    "Vector component filter not supported with virtual row variables!");
        }
        if (!componentFilter.getColumnArrays().isEmpty()) {
            throw new UnsupportedOperationException("Vector component filter not supported with column arrays!");
        }
    }

    @Override
    public @NotNull WritableRowSet filter(@NotNull final RowSet selection, @NotNull final RowSet fullSet,
            @NotNull final Table table, final boolean usePrev) {
        final ColumnSource<?> columnSource = table.getColumnSource(columnName);

        try (final VectorChunkFilter vectorFilter = vectorChunkFilterFactory.get()) {
            return ChunkFilter.applyChunkFilter(selection, columnSource, usePrev, vectorFilter);
        }
    }

    @Override
    public boolean isSimpleFilter() {
        return componentFilter.isSimpleFilter();
    }

    @Override
    public void setRecomputeListener(final RecomputeListener result) {}

    @Override
    public WhereFilter copy() {
        return new VectorComponentFilterWrapper(columnName, isArray, componentType, componentFilter.copy());
    }

    /**
     * If the component filter exposes a chunk filter, then wrap it in a VectorComponentFilterWrapper
     *
     * @param componentFilter the component filter to wrap
     * @param tableDefinition the definition of the table
     * @param columnName the name of the column in the table
     * @param isArray true if the column is an array, if false, then the column must be a vector
     * @param componentType the component type of the column
     * @return a VectorComponentFilterWrapper if one can be made from the component filter, otherwise null
     */
    public static VectorComponentFilterWrapper maybeCreateFilterWrapper(final WhereFilter componentFilter,
            final TableDefinition tableDefinition,
            final String columnName,
            final boolean isArray,
            final Class<?> componentType) {
        if (!(componentFilter instanceof ExposesChunkFilter)) {
            return null;
        }

        componentFilter.init(replaceDefinition(columnName, tableDefinition));
        final Optional<ChunkFilter> chunkFilter = ((ExposesChunkFilter) componentFilter).chunkFilter();
        return chunkFilter
                .map(cf -> new VectorComponentFilterWrapper(columnName, isArray, componentType,
                        componentFilter.copy()))
                .orElse(null);
    }

    static TableDefinition replaceDefinition(final String colName, final TableDefinition tableDefinition) {
        final ColumnDefinition<?> column = tableDefinition.getColumn(colName);
        final Class<?> componentType = column.getComponentType();
        if (componentType == null) {
            throw new IllegalArgumentException(
                    "Column " + colName + " has no component type for VectorComponentFilterWrapper!");
        }
        final ColumnDefinition<?> replacementColumn = column.withDataType(componentType);
        return TableDefinition.of(tableDefinition.getColumns().stream()
                .map(cd -> cd.getName().equals(colName) ? replacementColumn : cd).toArray(ColumnDefinition[]::new));
    }
}
