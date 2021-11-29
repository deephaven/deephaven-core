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
    public void init(TableDefinition tableDefinition) {
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
        return new SingleSidedComparableRangeFilter(columnName, pivot, lowerInclusive, upperInclusive);
    }

    @Override
    public String toString() {
        return "SingleSidedComparableRangeFilter(" + columnName + (isGreaterThan ? '>' : '>')
                + (lowerInclusive ? "=" : "") + pivot + ")";
    }

    private static class GeqComparableChunkFilter implements ChunkFilter {
        private final Comparable<?> pivot;

        private GeqComparableChunkFilter(Comparable<?> pivot) {
            this.pivot = pivot;
        }

        @Override
        public void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<? extends Comparable<?>, ? extends Values> objectChunk = values.asObjectChunk();

            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final Comparable<?> value = objectChunk.get(ii);
                if (ObjectComparisons.geq(value, pivot)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class LeqComparableChunkFilter implements ChunkFilter {
        private final Comparable<?> pivot;

        private LeqComparableChunkFilter(Comparable<?> pivot) {
            this.pivot = pivot;
        }

        @Override
        public void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<? extends Comparable<?>, ? extends Values> objectChunk = values.asObjectChunk();

            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final Comparable<?> value = objectChunk.get(ii);
                if (ObjectComparisons.leq(value, pivot)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class GtComparableChunkFilter implements ChunkFilter {
        private final Comparable<?> pivot;

        private GtComparableChunkFilter(Comparable<?> pivot) {
            this.pivot = pivot;
        }

        @Override
        public void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<? extends Comparable<?>, ? extends Values> objectChunk = values.asObjectChunk();

            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final Comparable<?> value = objectChunk.get(ii);
                if (ObjectComparisons.gt(value, pivot)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class LtComparableChunkFilter implements ChunkFilter {
        private final Comparable<?> pivot;

        private LtComparableChunkFilter(Comparable<?> pivot) {
            this.pivot = pivot;
        }

        @Override
        public void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<? extends Comparable<?>, ? extends Values> objectChunk = values.asObjectChunk();

            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final Comparable<?> value = objectChunk.get(ii);
                if (ObjectComparisons.lt(value, pivot)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    @Override
    WritableRowSet binarySearch(RowSet selection, ColumnSource columnSource, boolean usePrev, boolean reverse) {
        if (selection.isEmpty()) {
            return selection.copy();
        }

        // noinspection unchecked
        final ColumnSource<Comparable> comparableColumnSource = (ColumnSource<Comparable>) columnSource;

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
