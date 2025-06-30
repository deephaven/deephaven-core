//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.chunkfilter.ObjectChunkFilter;
import io.deephaven.util.annotations.InternalUseOnly;
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

        initChunkFilter();
    }

    ChunkFilter initChunkFilter() {
        return chunkFilter = makeComparableChunkFilter(lower, upper, lowerInclusive, upperInclusive);
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


    private static abstract class ComparableObjectChunkFilter extends ObjectChunkFilter<Comparable<?>> {
        final Comparable<?> lower;
        final Comparable<?> upper;

        private ComparableObjectChunkFilter(Comparable<?> lower, Comparable<?> upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    private final static class InclusiveInclusiveComparableChunkFilter extends ComparableObjectChunkFilter {

        private InclusiveInclusiveComparableChunkFilter(Comparable<?> lower, Comparable<?> upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(Comparable<?> value) {
            return ObjectComparisons.leq(lower, value) && ObjectComparisons.geq(upper, value);
        }

        @Override
        public boolean overlaps(Comparable<?> inputLower, Comparable<?> inputUpper) {
            return ObjectComparisons.geq(inputUpper, lower) && ObjectComparisons.geq(upper, inputLower);
        }
    }

    private final static class InclusiveExclusiveComparableChunkFilter extends ComparableObjectChunkFilter {

        private InclusiveExclusiveComparableChunkFilter(Comparable<?> lower, Comparable<?> upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(Comparable<?> value) {
            return ObjectComparisons.leq(lower, value) && ObjectComparisons.gt(upper, value);
        }

        @Override
        public boolean overlaps(Comparable<?> inputLower, Comparable<?> inputUpper) {
            return ObjectComparisons.geq(inputUpper, lower) && ObjectComparisons.gt(upper, inputLower);
        }
    }

    private final static class ExclusiveInclusiveComparableChunkFilter extends ComparableObjectChunkFilter {

        private ExclusiveInclusiveComparableChunkFilter(Comparable<?> lower, Comparable<?> upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(Comparable<?> value) {
            return ObjectComparisons.lt(lower, value) && ObjectComparisons.geq(upper, value);
        }

        @Override
        public boolean overlaps(Comparable<?> inputLower, Comparable<?> inputUpper) {
            return ObjectComparisons.gt(inputUpper, lower) && ObjectComparisons.geq(upper, inputLower);
        }
    }

    private final static class ExclusiveExclusiveComparableChunkFilter extends ComparableObjectChunkFilter {

        private ExclusiveExclusiveComparableChunkFilter(Comparable<?> lower, Comparable<?> upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(Comparable<?> value) {
            return ObjectComparisons.lt(lower, value) && ObjectComparisons.gt(upper, value);
        }

        @Override
        public boolean overlaps(Comparable<?> inputLower, Comparable<?> inputUpper) {
            return ObjectComparisons.gt(inputUpper, lower) && ObjectComparisons.gt(upper, inputLower);
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

    /**
     * Returns {@code true} if the range filter overlaps with the input range, else {@code false}
     *
     * @param inputLower the lower bound of the input range (inclusive)
     * @param inputUpper the upper bound of the input range (inclusive)
     *
     * @throws IllegalStateException if the chunk filter is not initialized
     */
    @InternalUseOnly
    public boolean overlaps(final Comparable<?> inputLower, final Comparable<?> inputUpper) {
        if (chunkFilter().isEmpty()) {
            throw new IllegalStateException("Chunk filter not initialized for: " + this);
        }
        // noinspection unchecked
        return ((ObjectChunkFilter<Comparable<?>>) chunkFilter().get()).overlaps(inputLower, inputUpper);
    }

    /**
     * Returns {@code true} if the given value is found within the range filter, else {@code false}.
     *
     * @param value the value to check
     *
     * @throws IllegalStateException if the chunk filter is not initialized
     */
    @InternalUseOnly
    public boolean matches(final Comparable<?> value) {
        if (chunkFilter().isEmpty()) {
            throw new IllegalStateException("Chunk filter not initialized for: " + this);
        }
        // noinspection unchecked
        return ((ObjectChunkFilter<Comparable<?>>) chunkFilter().get()).matches(value);
    }
}
