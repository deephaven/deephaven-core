/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkMatchFilterFactory and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;
import gnu.trove.set.hash.TLongHashSet;

/**
 * Creates chunk filters for long values.
 *
 * The strategy is that for one, two, or three values we have specialized
 * classes that will do the appropriate simple equality check.
 *
 * For more values, we use a trove set and check contains for each value in the chunk.
 */
public class LongChunkMatchFilterFactory {
    private LongChunkMatchFilterFactory() {} // static use only

    public static ChunkFilter.LongChunkFilter makeFilter(boolean invertMatch, long ... values) {
        if (invertMatch) {
            if (values.length == 1) {
                return new InverseSingleValueLongChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new InverseTwoValueLongChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new InverseThreeValueLongChunkFilter(values[0], values[1], values[2]);
            }
            return new InverseMultiValueLongChunkFilter(values);
        } else {
            if (values.length == 1) {
                return new SingleValueLongChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new TwoValueLongChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new ThreeValueLongChunkFilter(values[0], values[1], values[2]);
            }
            return new MultiValueLongChunkFilter(values);
        }
    }

    private static class SingleValueLongChunkFilter implements ChunkFilter.LongChunkFilter {
        private final long value;

        private SingleValueLongChunkFilter(long value) {
            this.value = value;
        }

        @Override
        public void filter(LongChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (values.get(ii) == value) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseSingleValueLongChunkFilter implements ChunkFilter.LongChunkFilter {
        private final long value;

        private InverseSingleValueLongChunkFilter(long value) {
            this.value = value;
        }

        @Override
        public void filter(LongChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (values.get(ii) != value) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class TwoValueLongChunkFilter implements ChunkFilter.LongChunkFilter {
        private final long value1;
        private final long value2;

        private TwoValueLongChunkFilter(long value1, long value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public void filter(LongChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final long checkValue = values.get(ii);
                if (checkValue == value1 || checkValue == value2) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseTwoValueLongChunkFilter implements ChunkFilter.LongChunkFilter {
        private final long value1;
        private final long value2;

        private InverseTwoValueLongChunkFilter(long value1, long value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public void filter(LongChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final long checkValue = values.get(ii);
                if (!(checkValue == value1 || checkValue == value2)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class ThreeValueLongChunkFilter implements ChunkFilter.LongChunkFilter {
        private final long value1;
        private final long value2;
        private final long value3;

        private ThreeValueLongChunkFilter(long value1, long value2, long value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public void filter(LongChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final long checkValue = values.get(ii);
                if (checkValue == value1 || checkValue == value2 || checkValue == value3) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseThreeValueLongChunkFilter implements ChunkFilter.LongChunkFilter {
        private final long value1;
        private final long value2;
        private final long value3;

        private InverseThreeValueLongChunkFilter(long value1, long value2, long value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public void filter(LongChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final long checkValue = values.get(ii);
                if (!(checkValue == value1 || checkValue == value2 || checkValue == value3)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class MultiValueLongChunkFilter implements ChunkFilter.LongChunkFilter {
        private final TLongHashSet values;

        private MultiValueLongChunkFilter(long ... values) {
            this.values = new TLongHashSet(values);
        }

        @Override
        public void filter(LongChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final long checkValue = values.get(ii);
                if (this.values.contains(checkValue)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseMultiValueLongChunkFilter implements ChunkFilter.LongChunkFilter {
        private final TLongHashSet values;

        private InverseMultiValueLongChunkFilter(long ... values) {
            this.values = new TLongHashSet(values);
        }

        @Override
        public void filter(LongChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final long checkValue = values.get(ii);
                if (!this.values.contains(checkValue)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }
}