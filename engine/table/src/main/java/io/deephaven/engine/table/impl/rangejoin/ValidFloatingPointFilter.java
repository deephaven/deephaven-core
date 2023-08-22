package io.deephaven.engine.table.impl.rangejoin;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Strings;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterImpl;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Objects;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;

/**
 * {@link WhereFilter} implementation that accepts non-{@code NaN}, non-{@code null} {@code doubles} or {@code floats}.
 */
class ValidFloatingPointFilter extends WhereFilterImpl {

    private final ColumnName columnName;

    ValidFloatingPointFilter(@NotNull final ColumnName columnName) {
        this.columnName = columnName;
    }

    @Override
    public List<String> getColumns() {
        return List.of(columnName.name());
    }

    @Override
    public List<String> getColumnArrays() {
        return List.of();
    }

    @Override
    public void init(@NotNull final TableDefinition tableDefinition) {
        final ColumnDefinition<?> columnDefinition = tableDefinition.getColumn(columnName.name());
        if (columnDefinition == null) {
            throw new IllegalArgumentException(String.format("Missing expected input column %s",
                    Strings.of(columnName)));
        }
        if (columnDefinition.getDataType() != double.class && columnDefinition.getDataType() != float.class) {
            throw new IllegalArgumentException(String.format(
                    "Unexpected data type for input column %s, expected double or float, found %s",
                    Strings.of(columnName), columnDefinition.getDataType()));
        }
    }

    @NotNull
    @Override
    public WritableRowSet filter(
            @NotNull final RowSet selection,
            @NotNull final RowSet fullSet,
            @NotNull final Table table,
            final boolean usePrev) {
        final ColumnSource<String> columnSource = table.getColumnSource(columnName.name());
        final ChunkFilter chunkFilter;
        switch (columnSource.getChunkType()) {
            case Double:
                chunkFilter = DoubleFilter.INSTANCE;
                break;
            case Float:
                chunkFilter = FloatFilter.INSTANCE;
                break;
            default:
                throw new IllegalStateException(String.format(
                        "Unexpected chunk type to filter: %s", columnSource.getChunkType()));
        }
        return ChunkFilter.applyChunkFilter(selection, columnSource, usePrev, chunkFilter);
    }

    private static final class DoubleFilter implements ChunkFilter.DoubleChunkFilter {

        private static final DoubleFilter INSTANCE = new DoubleFilter();

        private DoubleFilter() {}

        @Override
        public void filter(
                @NotNull final DoubleChunk<? extends Values> values,
                @NotNull final LongChunk<OrderedRowKeys> rowKeys,
                @NotNull final WritableLongChunk<OrderedRowKeys> acceptedRowKeys) {
            final int size = values.size();
            acceptedRowKeys.setSize(0);
            for (int vi = 0; vi < size; ++vi) {
                final double value = values.get(vi);
                if (!Double.isNaN(value) && value != NULL_DOUBLE) {
                    acceptedRowKeys.add(rowKeys.get(vi));
                }
            }
        }
    }

    private static final class FloatFilter implements ChunkFilter.FloatChunkFilter {

        private static final FloatFilter INSTANCE = new FloatFilter();

        private FloatFilter() {}

        @Override
        public void filter(
                @NotNull final FloatChunk<? extends Values> values,
                @NotNull final LongChunk<OrderedRowKeys> rowKeys,
                @NotNull final WritableLongChunk<OrderedRowKeys> acceptedRowKeys) {
            final int size = values.size();
            acceptedRowKeys.setSize(0);
            for (int vi = 0; vi < size; ++vi) {
                final float value = values.get(vi);
                if (!Float.isNaN(value) && value != NULL_FLOAT) {
                    acceptedRowKeys.add(rowKeys.get(vi));
                }
            }
        }
    }

    @Override
    public boolean isSimpleFilter() {
        return true;
    }

    @Override
    public void setRecomputeListener(final RecomputeListener result) {}

    @Override
    public boolean canMemoize() {
        return true;
    }

    @Override
    public WhereFilter copy() {
        return this;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final ValidFloatingPointFilter that = (ValidFloatingPointFilter) other;
        return Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName);
    }
}
