//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;

public class FloatRangeComparator {
    private FloatRangeComparator() {} // static use only

    private abstract static class FloatFloatFilter implements ChunkFilter.FloatChunkFilter {
        final float lower;
        final float upper;

        FloatFloatFilter(float lower, float upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    static class FloatDoubleInclusiveInclusiveFilter extends FloatFloatFilter {
        private FloatDoubleInclusiveInclusiveFilter(float lower, float upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(float value) {
            return FloatComparisons.geq(value, lower) && FloatComparisons.leq(value, upper);
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
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final FloatChunk<? extends Values> floatChunk = values.asFloatChunk();
            final int len = floatChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(floatChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    static class FloatDoubleInclusiveExclusiveFilter extends FloatFloatFilter {
        private FloatDoubleInclusiveExclusiveFilter(float lower, float upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(float value) {
            return FloatComparisons.geq(value, lower) && FloatComparisons.lt(value, upper);
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
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final FloatChunk<? extends Values> floatChunk = values.asFloatChunk();
            final int len = floatChunk.size();
            int count = 0;

            for (int ii = 0; ii < len; ++ii) {
                if (results.get(ii) && !matches(floatChunk.get(ii))) {
                    results.set(ii, false);
                    count++;
                }
            }
            return count;
        }
    }

    static class FloatDoubleExclusiveInclusiveFilter extends FloatFloatFilter {
        private FloatDoubleExclusiveInclusiveFilter(float lower, float upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(float value) {
            return FloatComparisons.gt(value, lower) && FloatComparisons.leq(value, upper);
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
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final FloatChunk<? extends Values> floatChunk = values.asFloatChunk();
            final int len = floatChunk.size();
            int count = 0;

            for (int ii = 0; ii < len; ++ii) {
                if (results.get(ii) && !matches(floatChunk.get(ii))) {
                    results.set(ii, false);
                    count++;
                }
            }
            return count;
        }
    }

    static class FloatDoubleExclusiveExclusiveFilter extends FloatFloatFilter {
        private FloatDoubleExclusiveExclusiveFilter(float lower, float upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(float value) {
            return FloatComparisons.gt(value, lower) && FloatComparisons.lt(value, upper);
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
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final FloatChunk<? extends Values> floatChunk = values.asFloatChunk();
            final int len = floatChunk.size();
            int count = 0;

            for (int ii = 0; ii < len; ++ii) {
                if (results.get(ii) && !matches(floatChunk.get(ii))) {
                    results.set(ii, false);
                    count++;
                }
            }
            return count;
        }
    }

    public static ChunkFilter.FloatChunkFilter makeFloatFilter(float lower, float upper, boolean lowerInclusive,
            boolean upperInclusive) {
        if (lowerInclusive) {
            if (upperInclusive) {
                return new FloatDoubleInclusiveInclusiveFilter(lower, upper);
            } else {
                return new FloatDoubleInclusiveExclusiveFilter(lower, upper);
            }
        } else {
            if (upperInclusive) {
                return new FloatDoubleExclusiveInclusiveFilter(lower, upper);
            } else {
                return new FloatDoubleExclusiveExclusiveFilter(lower, upper);
            }
        }
    }
}
