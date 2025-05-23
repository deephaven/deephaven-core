//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.chunkfilter.ObjectChunkFilter;
import io.deephaven.util.compare.ObjectComparisons;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;

public class ComparableRangeFilter extends AbstractRangeFilter {
    private final Comparable<?> upper;
    private final Comparable<?> lower;

    ComparableRangeFilter(String columnName, Comparable<?> val1, Comparable<?> val2, boolean lowerInclusive,
            boolean upperInclusive) {
        super(columnName, lowerInclusive, upperInclusive);

        if (ObjectComparisons.compare(val1, val2) > 0) {
            upper = val1;
            lower = val2;
        } else {
            upper = val2;
            lower = val1;
        }
    }

    @TestUseOnly
    public static ComparableRangeFilter makeForTest(String columnName, Comparable<?> lower, Comparable<?> upper,
            boolean lowerInclusive, boolean upperInclusive) {
        return new ComparableRangeFilter(columnName, lower, upper, lowerInclusive, upperInclusive);
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

        chunkFilter = makeComparableChunkFilter(lower, upper, lowerInclusive, upperInclusive);
    }

    public static ChunkFilter makeComparableChunkFilter(
            Comparable<?> lower, Comparable<?> upper, boolean lowerInclusive, boolean upperInclusive) {
        if (lowerInclusive) {
            if (upperInclusive) {
                return new InclusiveInclusiveComparableChunkFilter(lower, upper);
            } else {
                return new InclusiveExclusiveComparableChunkFilter(lower, upper);
            }
        } else {
            if (upperInclusive) {
                return new ExclusiveInclusiveComparableChunkFilter(lower, upper);
            } else {
                return new ExclusiveExclusiveComparableChunkFilter(lower, upper);
            }
        }
    }

    @Override
    public WhereFilter copy() {
        final ComparableRangeFilter copy =
                new ComparableRangeFilter(columnName, lower, upper, lowerInclusive, upperInclusive);
        copy.chunkFilter = chunkFilter;
        copy.longFilter = longFilter;
        return copy;
    }

    @Override
    public String toString() {
        return "ComparableRangeFilter(" + columnName + " in " +
                (lowerInclusive ? "[" : "(") + lower + "," + upper +
                (upperInclusive ? "]" : ")") + ")";
    }

    private final static class InclusiveInclusiveComparableChunkFilter
            extends ObjectChunkFilter<Comparable<?>> {
        private final Comparable<?> lower;
        private final Comparable<?> upper;

        private InclusiveInclusiveComparableChunkFilter(Comparable<?> lower, Comparable<?> upper) {
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public boolean matches(Comparable<?> value) {
            return ObjectComparisons.compare(lower, value) <= 0 && ObjectComparisons.compare(upper, value) >= 0;
        }
    }

    private final static class InclusiveExclusiveComparableChunkFilter
            extends ObjectChunkFilter<Comparable<?>> {
        private final Comparable<?> lower;
        private final Comparable<?> upper;

        private InclusiveExclusiveComparableChunkFilter(Comparable<?> lower, Comparable<?> upper) {
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public boolean matches(Comparable<?> value) {
            return ObjectComparisons.compare(lower, value) <= 0 && ObjectComparisons.compare(upper, value) > 0;
        }
    }

    private final static class ExclusiveInclusiveComparableChunkFilter
            extends ObjectChunkFilter<Comparable<?>> {
        private final Comparable<?> lower;
        private final Comparable<?> upper;

        private ExclusiveInclusiveComparableChunkFilter(Comparable<?> lower, Comparable<?> upper) {
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public boolean matches(Comparable<?> value) {
            return ObjectComparisons.compare(lower, value) < 0 && ObjectComparisons.compare(upper, value) >= 0;
        }
    }

    private final static class ExclusiveExclusiveComparableChunkFilter
            extends ObjectChunkFilter<Comparable<?>> {
        private final Comparable<?> lower;
        private final Comparable<?> upper;

        private ExclusiveExclusiveComparableChunkFilter(Comparable<?> lower, Comparable<?> upper) {
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public boolean matches(Comparable<?> value) {
            return ObjectComparisons.compare(lower, value) < 0 && ObjectComparisons.compare(upper, value) > 0;
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

        final Comparable<?> startValue = reverse ? upper : lower;
        final Comparable<?> endValue = reverse ? lower : upper;
        final boolean startInclusive = reverse ? upperInclusive : lowerInclusive;
        final boolean endInclusive = reverse ? lowerInclusive : upperInclusive;
        final int compareSign = reverse ? -1 : 1;

        long lowerBoundMin = bound(selection, usePrev, comparableColumnSource, 0, selection.size(), startValue,
                startInclusive, compareSign, false);
        long upperBoundMin = bound(selection, usePrev, comparableColumnSource, lowerBoundMin, selection.size(),
                endValue, endInclusive, compareSign, true);

        return selection.subSetByPositionRange(lowerBoundMin, upperBoundMin);
    }


    static long bound(RowSet selection, boolean usePrev, ColumnSource<Comparable<?>> comparableColumnSource,
            long minPosition, long maxPosition, Comparable<?> targetValue, boolean inclusive, int compareSign,
            boolean end) {
        while (minPosition < maxPosition) {
            final long midPos = (minPosition + maxPosition) / 2;
            final long midIdx = selection.get(midPos);

            final Comparable<?> compareValue =
                    usePrev ? comparableColumnSource.getPrev(midIdx) : comparableColumnSource.get(midIdx);
            final int compareResult = compareSign * ObjectComparisons.compare(compareValue, targetValue);

            if (compareResult < 0) {
                minPosition = midPos + 1;
            } else if (compareResult > 0) {
                maxPosition = midPos;
            } else {
                if (end) {
                    if (inclusive) {
                        minPosition = midPos + 1;
                    } else {
                        maxPosition = midPos;
                    }
                } else {
                    if (inclusive) {
                        maxPosition = midPos;
                    } else {
                        minPosition = midPos + 1;
                    }
                }
            }
        }
        return minPosition;
    }

    @Override
    public boolean overlaps(
            @NotNull final Object lower,
            @NotNull final Object upper,
            final boolean lowerInclusive,
            final boolean upperInclusive) {

        final int c1 = CompareUtils.compare(this.lower, upper);
        if (c1 > 0) {
            return false; // this.lower > inputUpper, no overlap possible.
        }
        final int c2 = CompareUtils.compare(lower, this.upper);
        if (c2 > 0) {
            return false; // inputLower > this.upper, no overlap possible.
        }
        // There is no overlap inside the ranges, test the edges.
        return (c1 < 0 && c2 < 0)
                || (c1 == 0 && this.lowerInclusive && upperInclusive)
                || (c2 == 0 && lowerInclusive && this.upperInclusive);
    }

    @Override
    public boolean contains(@NotNull final Object value) {
        final int c1 = CompareUtils.compare(this.lower, value);
        if (c1 > 0) {
            return false; // this.lower > value, no overlap possible.
        }
        final int c2 = CompareUtils.compare(value, this.upper);
        if (c2 > 0) {
            return false; // value > this.upper, no overlap possible.
        }
        // There is no overlap inside the ranges, test the edges.
        return (c1 < 0 && c2 < 0) || (c1 == 0 && this.lowerInclusive) || (c2 == 0 && this.upperInclusive);
    }
}
