//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatRangeComparator and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.mutable.MutableInt;

import java.util.function.LongConsumer;

public class DoubleRangeComparator {
    private DoubleRangeComparator() {} // static use only

    private abstract static class DoubleDoubleFilter implements ChunkFilter.DoubleChunkFilter {
        final double lower;
        final double upper;

        DoubleDoubleFilter(double lower, double upper) {
            this.lower = lower;
            this.upper = upper;
        }

        abstract public void filter(DoubleChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results);

        abstract public void filter(DoubleChunk<? extends Values> values, RowSequence rows, LongConsumer consumer);
    }

    static class DoubleDoubleInclusiveInclusiveFilter extends DoubleDoubleFilter {
        private DoubleDoubleInclusiveInclusiveFilter(double lower, double upper) {
            super(lower, upper);
        }

        @Override
        public void filter(DoubleChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final double value = values.get(ii);
                if (DoubleComparisons.geq(value, lower) && DoubleComparisons.leq(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public void filter(DoubleChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys(row -> {
                final double value = values.get(index.getAndIncrement());
                if (DoubleComparisons.geq(value, lower) && DoubleComparisons.leq(value, upper)) {
                    consumer.accept(row);
                }
            });
        }
    }

    static class DoubleDoubleInclusiveExclusiveFilter extends DoubleDoubleFilter {
        private DoubleDoubleInclusiveExclusiveFilter(double lower, double upper) {
            super(lower, upper);
        }

        @Override
        public void filter(DoubleChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final double value = values.get(ii);
                if (DoubleComparisons.geq(value, lower) && DoubleComparisons.lt(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public void filter(DoubleChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys(row -> {
                final double value = values.get(index.getAndIncrement());
                if (DoubleComparisons.geq(value, lower) && DoubleComparisons.lt(value, upper)) {
                    consumer.accept(row);
                }
            });
        }
    }

    static class DoubleDoubleExclusiveInclusiveFilter extends DoubleDoubleFilter {
        private DoubleDoubleExclusiveInclusiveFilter(double lower, double upper) {
            super(lower, upper);
        }

        @Override
        public void filter(DoubleChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final double value = values.get(ii);
                if (DoubleComparisons.gt(value, lower) && DoubleComparisons.leq(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public void filter(DoubleChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys(row -> {
                final double value = values.get(index.getAndIncrement());
                if (DoubleComparisons.gt(value, lower) && DoubleComparisons.leq(value, upper)) {
                    consumer.accept(row);
                }
            });
        }
    }

    static class DoubleDoubleExclusiveExclusiveFilter extends DoubleDoubleFilter {
        private DoubleDoubleExclusiveExclusiveFilter(double lower, double upper) {
            super(lower, upper);
        }

        @Override
        public void filter(DoubleChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final double value = values.get(ii);
                if (DoubleComparisons.gt(value, lower) && DoubleComparisons.lt(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public void filter(DoubleChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys(row -> {
                final double value = values.get(index.getAndIncrement());
                if (DoubleComparisons.gt(value, lower) && DoubleComparisons.lt(value, upper)) {
                    consumer.accept(row);
                }
            });
        }
    }

    public static ChunkFilter.DoubleChunkFilter makeDoubleFilter(double lower, double upper, boolean lowerInclusive,
            boolean upperInclusive) {
        if (lowerInclusive) {
            if (upperInclusive) {
                return new DoubleDoubleInclusiveInclusiveFilter(lower, upper);
            } else {
                return new DoubleDoubleInclusiveExclusiveFilter(lower, upper);
            }
        } else {
            if (upperInclusive) {
                return new DoubleDoubleExclusiveInclusiveFilter(lower, upper);
            } else {
                return new DoubleDoubleExclusiveExclusiveFilter(lower, upper);
            }
        }
    }
}
