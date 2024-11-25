//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.mutable.MutableInt;

import java.util.function.LongConsumer;

public class FloatRangeComparator {
    private FloatRangeComparator() {} // static use only

    private abstract static class FloatFloatFilter implements ChunkFilter.FloatChunkFilter {
        final float lower;
        final float upper;

        FloatFloatFilter(float lower, float upper) {
            this.lower = lower;
            this.upper = upper;
        }

        abstract public void filter(FloatChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results);

        abstract public void filter(FloatChunk<? extends Values> values, RowSequence rows, LongConsumer consumer);
    }

    static class FloatDoubleInclusiveInclusiveFilter extends FloatFloatFilter {
        private FloatDoubleInclusiveInclusiveFilter(float lower, float upper) {
            super(lower, upper);
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final float value = values.get(ii);
                if (FloatComparisons.geq(value, lower) && FloatComparisons.leq(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys(row -> {
                final float value = values.get(index.getAndIncrement());
                if (FloatComparisons.geq(value, lower) && FloatComparisons.leq(value, upper)) {
                    consumer.accept(row);
                }
            });
        }
    }

    static class FloatDoubleInclusiveExclusiveFilter extends FloatFloatFilter {
        private FloatDoubleInclusiveExclusiveFilter(float lower, float upper) {
            super(lower, upper);
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final float value = values.get(ii);
                if (FloatComparisons.geq(value, lower) && FloatComparisons.lt(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys(row -> {
                final float value = values.get(index.getAndIncrement());
                if (FloatComparisons.geq(value, lower) && FloatComparisons.lt(value, upper)) {
                    consumer.accept(row);
                }
            });
        }
    }

    static class FloatDoubleExclusiveInclusiveFilter extends FloatFloatFilter {
        private FloatDoubleExclusiveInclusiveFilter(float lower, float upper) {
            super(lower, upper);
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final float value = values.get(ii);
                if (FloatComparisons.gt(value, lower) && FloatComparisons.leq(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys(row -> {
                final float value = values.get(index.getAndIncrement());
                if (FloatComparisons.gt(value, lower) && FloatComparisons.leq(value, upper)) {
                    consumer.accept(row);
                }
            });
        }
    }

    static class FloatDoubleExclusiveExclusiveFilter extends FloatFloatFilter {
        private FloatDoubleExclusiveExclusiveFilter(float lower, float upper) {
            super(lower, upper);
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final float value = values.get(ii);
                if (FloatComparisons.gt(value, lower) && FloatComparisons.lt(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys(row -> {
                final float value = values.get(index.getAndIncrement());
                if (FloatComparisons.gt(value, lower) && FloatComparisons.lt(value, upper)) {
                    consumer.accept(row);
                }
            });
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
