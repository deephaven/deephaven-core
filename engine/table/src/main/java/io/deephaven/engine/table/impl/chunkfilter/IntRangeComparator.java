//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRangeComparator and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.compare.IntComparisons;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.mutable.MutableInt;

import java.util.function.LongConsumer;

public class IntRangeComparator {
    private IntRangeComparator() {} // static use only

    private abstract static class IntIntFilter implements ChunkFilter.IntChunkFilter {
        final int lower;
        final int upper;

        IntIntFilter(int lower, int upper) {
            this.lower = lower;
            this.upper = upper;
        }

        abstract public void filter(IntChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results);
    }

    static class IntIntInclusiveInclusiveFilter extends IntIntFilter {
        private IntIntInclusiveInclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        public void filter(IntChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final int value = values.get(ii);
                if (IntComparisons.geq(value, lower) && IntComparisons.leq(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }

        public void filter(IntChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys(row -> {
                final int value = values.get(index.getAndIncrement());
                if (IntComparisons.geq(value, lower) && IntComparisons.leq(value, upper)) {
                    consumer.accept(row);
                }
            });
        }
    }

    static class IntIntInclusiveExclusiveFilter extends IntIntFilter {
        private IntIntInclusiveExclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        public void filter(IntChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final int value = values.get(ii);
                if (IntComparisons.geq(value, lower) && IntComparisons.lt(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }

        public void filter(IntChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys(row -> {
                final int value = values.get(index.getAndIncrement());
                if (IntComparisons.geq(value, lower) && IntComparisons.lt(value, upper)) {
                    consumer.accept(row);
                }
            });
        }
    }

    static class IntIntExclusiveInclusiveFilter extends IntIntFilter {
        private IntIntExclusiveInclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        public void filter(IntChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final int value = values.get(ii);
                if (IntComparisons.gt(value, lower) && IntComparisons.leq(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }

        public void filter(IntChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys(row -> {
                final int value = values.get(index.getAndIncrement());
                if (IntComparisons.gt(value, lower) && IntComparisons.leq(value, upper)) {
                    consumer.accept(row);
                }
            });
        }
    }

    static class IntIntExclusiveExclusiveFilter extends IntIntFilter {
        private IntIntExclusiveExclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        public void filter(IntChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final int value = values.get(ii);
                if (IntComparisons.gt(value, lower) && IntComparisons.lt(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }

        public void filter(IntChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys(row -> {
                final int value = values.get(index.getAndIncrement());
                if (IntComparisons.gt(value, lower) && IntComparisons.lt(value, upper)) {
                    consumer.accept(row);
                }
            });
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
