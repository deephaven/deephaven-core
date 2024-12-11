//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRangeComparator and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.util.compare.IntComparisons;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;

public class IntRangeComparator {
    private IntRangeComparator() {} // static use only

    private abstract static class IntIntFilter implements ChunkFilter.IntChunkFilter {
        final int lower;
        final int upper;

        IntIntFilter(int lower, int upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    static class IntIntInclusiveInclusiveFilter extends IntIntFilter {
        private IntIntInclusiveInclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(int value) {
            return IntComparisons.geq(value, lower) && IntComparisons.leq(value, upper);
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
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = intChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(intChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = intChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(intChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    static class IntIntInclusiveExclusiveFilter extends IntIntFilter {
        private IntIntInclusiveExclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(int value) {
            return IntComparisons.geq(value, lower) && IntComparisons.lt(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = intChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(intChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = intChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(intChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    static class IntIntExclusiveInclusiveFilter extends IntIntFilter {
        private IntIntExclusiveInclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(int value) {
            return IntComparisons.gt(value, lower) && IntComparisons.leq(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = intChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(intChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = intChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(intChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    static class IntIntExclusiveExclusiveFilter extends IntIntFilter {
        private IntIntExclusiveExclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(int value) {
            return IntComparisons.gt(value, lower) && IntComparisons.lt(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = intChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(intChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = intChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(intChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    public static ChunkFilter.IntChunkFilter makeIntFilter(int lower, int upper, boolean lowerInclusive,
            boolean upperInclusive) {
        if (lowerInclusive) {
            if (upperInclusive) {
                return new IntIntInclusiveInclusiveFilter(lower, upper);
            } else {
                return new IntIntInclusiveExclusiveFilter(lower, upper);
            }
        } else {
            if (upperInclusive) {
                return new IntIntExclusiveInclusiveFilter(lower, upper);
            } else {
                return new IntIntExclusiveExclusiveFilter(lower, upper);
            }
        }
    }
}
