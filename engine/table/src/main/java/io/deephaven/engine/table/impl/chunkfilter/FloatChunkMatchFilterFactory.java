//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkMatchFilterFactory and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;
import gnu.trove.set.hash.TFloatHashSet;
import io.deephaven.util.mutable.MutableInt;

import java.util.function.LongConsumer;

/**
 * Creates chunk filters for float values.
 * <p>
 * The strategy is that for one, two, or three values we have specialized classes that will do the appropriate simple
 * equality check.
 * <p>
 * For more values, we use a trove set and check contains for each value in the chunk.
 */
public class FloatChunkMatchFilterFactory {
    private FloatChunkMatchFilterFactory() {} // static use only

    public static ChunkFilter.FloatChunkFilter makeFilter(boolean invertMatch, float... values) {
        if (invertMatch) {
            if (values.length == 1) {
                return new InverseSingleValueFloatChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new InverseTwoValueFloatChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new InverseThreeValueFloatChunkFilter(values[0], values[1], values[2]);
            }
            return new InverseMultiValueFloatChunkFilter(values);
        } else {
            if (values.length == 1) {
                return new SingleValueFloatChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new TwoValueFloatChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new ThreeValueFloatChunkFilter(values[0], values[1], values[2]);
            }
            return new MultiValueFloatChunkFilter(values);
        }
    }

    private static class SingleValueFloatChunkFilter implements ChunkFilter.FloatChunkFilter {
        private final float value;

        private SingleValueFloatChunkFilter(float value) {
            this.value = value;
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (values.get(ii) == value) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys((final long key) -> {
                if (values.get(index.getAndIncrement()) == value) {
                    consumer.accept(key);
                }
            });
        }
    }

    private static class InverseSingleValueFloatChunkFilter implements ChunkFilter.FloatChunkFilter {
        private final float value;

        private InverseSingleValueFloatChunkFilter(float value) {
            this.value = value;
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (values.get(ii) != value) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys((final long key) -> {
                if (values.get(index.getAndIncrement()) != value) {
                    consumer.accept(key);
                }
            });
        }
    }

    private static class TwoValueFloatChunkFilter implements ChunkFilter.FloatChunkFilter {
        private final float value1;
        private final float value2;

        private TwoValueFloatChunkFilter(float value1, float value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final float checkValue = values.get(ii);
                if (checkValue == value1 || checkValue == value2) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys((final long key) -> {
                final float checkValue = values.get(index.getAndIncrement());
                if (checkValue == value1 || checkValue == value2) {
                    consumer.accept(key);
                }
            });
        }
    }

    private static class InverseTwoValueFloatChunkFilter implements ChunkFilter.FloatChunkFilter {
        private final float value1;
        private final float value2;

        private InverseTwoValueFloatChunkFilter(float value1, float value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final float checkValue = values.get(ii);
                if (!(checkValue == value1 || checkValue == value2)) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys((final long key) -> {
                final float checkValue = values.get(index.getAndIncrement());
                if (!(checkValue == value1 || checkValue == value2)) {
                    consumer.accept(key);
                }
            });
        }
    }

    private static class ThreeValueFloatChunkFilter implements ChunkFilter.FloatChunkFilter {
        private final float value1;
        private final float value2;
        private final float value3;

        private ThreeValueFloatChunkFilter(float value1, float value2, float value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final float checkValue = values.get(ii);
                if (checkValue == value1 || checkValue == value2 || checkValue == value3) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys((final long key) -> {
                final float checkValue = values.get(index.getAndIncrement());
                if (checkValue == value1 || checkValue == value2 || checkValue == value3) {
                    consumer.accept(key);
                }
            });
        }
    }

    private static class InverseThreeValueFloatChunkFilter implements ChunkFilter.FloatChunkFilter {
        private final float value1;
        private final float value2;
        private final float value3;

        private InverseThreeValueFloatChunkFilter(float value1, float value2, float value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final float checkValue = values.get(ii);
                if (!(checkValue == value1 || checkValue == value2 || checkValue == value3)) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys((final long key) -> {
                final float checkValue = values.get(index.getAndIncrement());
                if (!(checkValue == value1 || checkValue == value2 || checkValue == value3)) {
                    consumer.accept(key);
                }
            });
        }
    }

    private static class MultiValueFloatChunkFilter implements ChunkFilter.FloatChunkFilter {
        private final TFloatHashSet values;

        private MultiValueFloatChunkFilter(float... values) {
            this.values = new TFloatHashSet(values);
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final float checkValue = values.get(ii);
                if (this.values.contains(checkValue)) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys((final long key) -> {
                final float checkValue = values.get(index.getAndIncrement());
                if (this.values.contains(checkValue)) {
                    consumer.accept(key);
                }
            });
        }
    }

    private static class InverseMultiValueFloatChunkFilter implements ChunkFilter.FloatChunkFilter {
        private final TFloatHashSet values;

        private InverseMultiValueFloatChunkFilter(float... values) {
            this.values = new TFloatHashSet(values);
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final float checkValue = values.get(ii);
                if (!this.values.contains(checkValue)) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public void filter(FloatChunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            final MutableInt index = new MutableInt(0);
            rows.forAllRowKeys((final long key) -> {
                final float checkValue = values.get(index.getAndIncrement());
                if (!this.values.contains(checkValue)) {
                    consumer.accept(key);
                }
            });
        }
    }
}
