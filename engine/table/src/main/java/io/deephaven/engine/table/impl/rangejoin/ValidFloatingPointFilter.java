//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.rangejoin;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Strings;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.chunkfilter.DoubleChunkFilter;
import io.deephaven.engine.table.impl.chunkfilter.FloatChunkFilter;
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

    private static final class DoubleFilter extends DoubleChunkFilter {

        private static final DoubleFilter INSTANCE = new DoubleFilter();

        private DoubleFilter() {}

        @Override
        public boolean matches(final double value) {
            return !Double.isNaN(value) && value != NULL_DOUBLE;
        }
    }

    private static final class FloatFilter extends FloatChunkFilter {

        private static final FloatFilter INSTANCE = new FloatFilter();

        private FloatFilter() {}

        @Override
        public boolean matches(final float value) {
            return !Float.isNaN(value) && value != NULL_FLOAT;
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
