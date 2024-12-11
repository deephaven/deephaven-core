//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.util.compare.ObjectComparisons;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;

public class SingleSidedComparableRangeFilter extends AbstractRangeFilter {
    private final Comparable<?> pivot;
    private final boolean isGreaterThan;

    SingleSidedComparableRangeFilter(String columnName, Comparable<?> val, boolean inclusive, boolean isGreaterThan) {
        super(columnName, inclusive, inclusive);
        this.isGreaterThan = isGreaterThan;
        pivot = val;
    }

    @TestUseOnly
    public static SingleSidedComparableRangeFilter makeForTest(String columnName, Comparable<?> value,
            boolean inclusive, boolean isGreaterThan) {
        return new SingleSidedComparableRangeFilter(columnName, value, inclusive, isGreaterThan);
    }

    @Override
    public void init(@NotNull final TableDefinition tableDefinition) {
        if (chunkFilter != null) {
            return;
        }

        final ColumnDefinition<?> def = tableDefinition.getColumn(columnName);
        if (def == null) {
            throw new RuntimeException("Column \"" + columnName + "\" doesn't exist in this table, available columns: "
                    + tableDefinition.getColumnNames());
        }

        Assert.assertion(Comparable.class.isAssignableFrom(def.getDataType()),
                "Comparable.class.isAssignableFrom(def.getDataType())", def.getDataType(), "def.getDataType()");

        chunkFilter = makeComparableChunkFilter(pivot, lowerInclusive, isGreaterThan);
    }

    public static ChunkFilter makeComparableChunkFilter(Comparable<?> pivot, boolean inclusive, boolean isGreaterThan) {
        if (inclusive) {
            if (isGreaterThan) {
                return new GeqComparableChunkFilter(pivot);
            } else {
                return new LeqComparableChunkFilter(pivot);
            }
        } else {
            if (isGreaterThan) {
                return new GtComparableChunkFilter(pivot);
            } else {
                return new LtComparableChunkFilter(pivot);
            }
        }
    }

    @Override
    public WhereFilter copy() {
        final SingleSidedComparableRangeFilter copy =
                new SingleSidedComparableRangeFilter(columnName, pivot, lowerInclusive, upperInclusive);
        copy.chunkFilter = chunkFilter;
        copy.longFilter = longFilter;
        return copy;
    }

    @Override
    public String toString() {
        return "SingleSidedComparableRangeFilter(" + columnName + (isGreaterThan ? '>' : '<')
                + (lowerInclusive ? "=" : "") + pivot + ")";
    }

    private static class GeqComparableChunkFilter implements ChunkFilter.ObjectChunkFilter<Comparable<?>> {
        private final Comparable<?> pivot;

        private GeqComparableChunkFilter(Comparable<?> pivot) {
            this.pivot = pivot;
        }

        @Override
        public boolean matches(Comparable<?> value) {
            return ObjectComparisons.geq(value, pivot);
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
            final ObjectChunk<? extends Comparable<?>, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = objectChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final ObjectChunk<? extends Comparable<?>, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = objectChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(objectChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    private static class LeqComparableChunkFilter implements ChunkFilter.ObjectChunkFilter<Comparable<?>> {
        private final Comparable<?> pivot;

        private LeqComparableChunkFilter(Comparable<?> pivot) {
            this.pivot = pivot;
        }

        @Override
        public boolean matches(Comparable<?> value) {
            return ObjectComparisons.leq(value, pivot);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<? extends Comparable<?>, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = objectChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final ObjectChunk<? extends Comparable<?>, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = objectChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(objectChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    private static class GtComparableChunkFilter implements ChunkFilter.ObjectChunkFilter<Comparable<?>> {
        private final Comparable<?> pivot;

        private GtComparableChunkFilter(Comparable<?> pivot) {
            this.pivot = pivot;
        }

        @Override
        public boolean matches(Comparable<?> value) {
            return ObjectComparisons.gt(value, pivot);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<? extends Comparable<?>, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = objectChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final ObjectChunk<? extends Comparable<?>, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = objectChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(objectChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    private static class LtComparableChunkFilter implements ChunkFilter.ObjectChunkFilter<Comparable<?>> {
        private final Comparable<?> pivot;

        private LtComparableChunkFilter(Comparable<?> pivot) {
            this.pivot = pivot;
        }

        @Override
        public boolean matches(Comparable<?> value) {
            return ObjectComparisons.lt(value, pivot);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<? extends Comparable<?>, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = objectChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final ObjectChunk<? extends Comparable<?>, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = objectChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(objectChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    @NotNull
    @Override
    WritableRowSet binarySearch(
            @NotNull final RowSet selection,
            @NotNull final ColumnSource<?> columnSource,
            final boolean usePrev,
            final boolean reverse) {
        if (selection.isEmpty()) {
            return selection.copy();
        }

        // noinspection unchecked
        final ColumnSource<Comparable<?>> comparableColumnSource = (ColumnSource<Comparable<?>>) columnSource;

        final int compareSign = reverse ? -1 : 1;
        long lowerBoundMin = ComparableRangeFilter.bound(selection, usePrev, comparableColumnSource, 0,
                selection.size(), pivot, lowerInclusive, compareSign, isGreaterThan == reverse);

        if (isGreaterThan == reverse) {
            return selection.subSetByPositionRange(0, lowerBoundMin);
        } else {
            return selection.subSetByPositionRange(lowerBoundMin, selection.size());
        }
    }
}
