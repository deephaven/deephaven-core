//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.compare.CharComparisons;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.mutable.MutableInt;

import java.util.function.LongConsumer;

public class CharRangeComparator {
    private CharRangeComparator() {} // static use only

    private abstract static class CharCharFilter implements ChunkFilter.CharChunkFilter {
        final char lower;
        final char upper;

        CharCharFilter(char lower, char upper) {
            this.lower = lower;
            this.upper = upper;
        }

        abstract public void filter(CharChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results);
    }

    static class CharCharInclusiveInclusiveFilter extends CharCharFilter {
        private CharCharInclusiveInclusiveFilter(char lower, char upper) {
            super(lower, upper);
        }

        public void filter(CharChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final char value = values.get(ii);
                if (CharComparisons.geq(value, lower) && CharComparisons.leq(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }

        public void filter(CharChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys(row -> {
                final char value = values.get(index.getAndIncrement());
                if (CharComparisons.geq(value, lower) && CharComparisons.leq(value, upper)) {
                    consumer.accept(row);
                }
            });
        }
    }

    static class CharCharInclusiveExclusiveFilter extends CharCharFilter {
        private CharCharInclusiveExclusiveFilter(char lower, char upper) {
            super(lower, upper);
        }

        public void filter(CharChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final char value = values.get(ii);
                if (CharComparisons.geq(value, lower) && CharComparisons.lt(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }

        public void filter(CharChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys(row -> {
                final char value = values.get(index.getAndIncrement());
                if (CharComparisons.geq(value, lower) && CharComparisons.lt(value, upper)) {
                    consumer.accept(row);
                }
            });
        }
    }

    static class CharCharExclusiveInclusiveFilter extends CharCharFilter {
        private CharCharExclusiveInclusiveFilter(char lower, char upper) {
            super(lower, upper);
        }

        public void filter(CharChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final char value = values.get(ii);
                if (CharComparisons.gt(value, lower) && CharComparisons.leq(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }

        public void filter(CharChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys(row -> {
                final char value = values.get(index.getAndIncrement());
                if (CharComparisons.gt(value, lower) && CharComparisons.leq(value, upper)) {
                    consumer.accept(row);
                }
            });
        }
    }

    static class CharCharExclusiveExclusiveFilter extends CharCharFilter {
        private CharCharExclusiveExclusiveFilter(char lower, char upper) {
            super(lower, upper);
        }

        public void filter(CharChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final char value = values.get(ii);
                if (CharComparisons.gt(value, lower) && CharComparisons.lt(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }

        public void filter(CharChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys(row -> {
                final char value = values.get(index.getAndIncrement());
                if (CharComparisons.gt(value, lower) && CharComparisons.lt(value, upper)) {
                    consumer.accept(row);
                }
            });
        }
    }

    public static ChunkFilter.CharChunkFilter makeCharFilter(char lower, char upper, boolean lowerInclusive,
            boolean upperInclusive) {
        if (lowerInclusive) {
            if (upperInclusive) {
                return new CharCharInclusiveInclusiveFilter(lower, upper);
            } else {
                return new CharCharInclusiveExclusiveFilter(lower, upper);
            }
        } else {
            if (upperInclusive) {
                return new CharCharExclusiveInclusiveFilter(lower, upper);
            } else {
                return new CharCharExclusiveExclusiveFilter(lower, upper);
            }
        }
    }
}
