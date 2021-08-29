package io.deephaven.engine.v2.select.chunkfilters;

import io.deephaven.engine.util.DhFloatComparisons;
import io.deephaven.engine.v2.select.ChunkFilter;
import io.deephaven.engine.v2.sources.chunk.*;
import io.deephaven.engine.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.engine.v2.sources.chunk.Attributes.Values;

public class FloatRangeComparator {
    private FloatRangeComparator() {} // static use only

    private abstract static class FloatFloatFilter implements ChunkFilter.FloatChunkFilter {
        final float lower;
        final float upper;

        FloatFloatFilter(float lower, float upper) {
            this.lower = lower;
            this.upper = upper;
        }

        abstract public void filter(FloatChunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys, WritableLongChunk<OrderedKeyIndices> results);
    }

    static class FloatDoubleInclusiveInclusiveFilter extends FloatFloatFilter {
        private FloatDoubleInclusiveInclusiveFilter(float lower, float upper) {
            super(lower, upper);
        }

        public void filter(FloatChunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys, WritableLongChunk<OrderedKeyIndices> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final float value = values.get(ii);
                if (DhFloatComparisons.geq(value, lower) && DhFloatComparisons.leq(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class FloatDoubleInclusiveExclusiveFilter extends FloatFloatFilter {
        private FloatDoubleInclusiveExclusiveFilter(float lower, float upper) {
            super(lower, upper);
        }

        public void filter(FloatChunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys, WritableLongChunk<OrderedKeyIndices> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final float value = values.get(ii);
                if (DhFloatComparisons.geq(value, lower) && DhFloatComparisons.lt(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class FloatDoubleExclusiveInclusiveFilter extends FloatFloatFilter {
        private FloatDoubleExclusiveInclusiveFilter(float lower, float upper) {
            super(lower, upper);
        }

        public void filter(FloatChunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys, WritableLongChunk<OrderedKeyIndices> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final float value = values.get(ii);
                if (DhFloatComparisons.gt(value, lower) && DhFloatComparisons.leq(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class FloatDoubleExclusiveExclusiveFilter extends FloatFloatFilter {
        private FloatDoubleExclusiveExclusiveFilter(float lower, float upper) {
            super(lower, upper);
        }

        public void filter(FloatChunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys, WritableLongChunk<OrderedKeyIndices> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final float value = values.get(ii);
                if (DhFloatComparisons.gt(value, lower) && DhFloatComparisons.lt(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    public static ChunkFilter.FloatChunkFilter makeFloatFilter(float lower, float upper, boolean lowerInclusive, boolean upperInclusive) {
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
