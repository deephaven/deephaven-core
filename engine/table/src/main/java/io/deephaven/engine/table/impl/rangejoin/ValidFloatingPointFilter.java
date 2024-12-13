//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.rangejoin;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Strings;
import io.deephaven.chunk.*;
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

        private boolean matches(final double value) {
            return !Double.isNaN(value) && value != NULL_DOUBLE;
        }

        /*
         * The following functions are identical and repeated for each of the filter types. This is to aid the JVM in
         * correctly inlining the matches() function. The goal is to have a single virtual call per chunk rather than
         * once per value. This improves performance on JVM <= 21, but may be unnecessary on newer JVMs.
         */
        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final DoubleChunk<? extends Values> doubleChunk = values.asDoubleChunk();
            final int len = doubleChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(doubleChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final DoubleChunk<? extends Values> typedChunk = values.asDoubleChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final DoubleChunk<? extends Values> typedChunk = values.asDoubleChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    continue; // already false, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 0 : 1;
            }
            return count;
        }

        @Override
        public int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final DoubleChunk<? extends Values> typedChunk = values.asDoubleChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    continue; // already true, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    private static final class FloatFilter implements ChunkFilter.FloatChunkFilter {

        private static final FloatFilter INSTANCE = new FloatFilter();

        private FloatFilter() {}

        private boolean matches(final float value) {
            return !Float.isNaN(value) && value != NULL_FLOAT;
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final FloatChunk<? extends Values> floatChunk = values.asFloatChunk();
            final int len = floatChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(floatChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final FloatChunk<? extends Values> typedChunk = values.asFloatChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final FloatChunk<? extends Values> typedChunk = values.asFloatChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    continue; // already false, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 0 : 1;
            }
            return count;
        }

        @Override
        public int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final FloatChunk<? extends Values> typedChunk = values.asFloatChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    continue; // already true, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
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
