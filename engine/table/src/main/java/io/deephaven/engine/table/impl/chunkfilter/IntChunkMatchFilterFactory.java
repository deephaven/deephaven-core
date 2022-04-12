/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkMatchFilterFactory and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Creates chunk filters for int values.
 *
 * The strategy is that for one, two, or three values we have specialized
 * classes that will do the appropriate simple equality check.
 *
 * For more values, we use a trove set and check contains for each value in the chunk.
 */
public class IntChunkMatchFilterFactory {
    private IntChunkMatchFilterFactory() {} // static use only

    public static ChunkFilter.IntChunkFilter makeFilter(boolean invertMatch, int ... values) {
        if (invertMatch) {
            if (values.length == 1) {
                return new InverseSingleValueIntChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new InverseTwoValueIntChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new InverseThreeValueIntChunkFilter(values[0], values[1], values[2]);
            }
            return new InverseMultiValueIntChunkFilter(values);
        } else {
            if (values.length == 1) {
                return new SingleValueIntChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new TwoValueIntChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new ThreeValueIntChunkFilter(values[0], values[1], values[2]);
            }
            return new MultiValueIntChunkFilter(values);
        }
    }

    private static class SingleValueIntChunkFilter implements ChunkFilter.IntChunkFilter {
        private final int value;

        private SingleValueIntChunkFilter(int value) {
            this.value = value;
        }

        @Override
        public void filter(IntChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (values.get(ii) == value) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseSingleValueIntChunkFilter implements ChunkFilter.IntChunkFilter {
        private final int value;

        private InverseSingleValueIntChunkFilter(int value) {
            this.value = value;
        }

        @Override
        public void filter(IntChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (values.get(ii) != value) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class TwoValueIntChunkFilter implements ChunkFilter.IntChunkFilter {
        private final int value1;
        private final int value2;

        private TwoValueIntChunkFilter(int value1, int value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public void filter(IntChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final int checkValue = values.get(ii);
                if (checkValue == value1 || checkValue == value2) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseTwoValueIntChunkFilter implements ChunkFilter.IntChunkFilter {
        private final int value1;
        private final int value2;

        private InverseTwoValueIntChunkFilter(int value1, int value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public void filter(IntChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final int checkValue = values.get(ii);
                if (!(checkValue == value1 || checkValue == value2)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class ThreeValueIntChunkFilter implements ChunkFilter.IntChunkFilter {
        private final int value1;
        private final int value2;
        private final int value3;

        private ThreeValueIntChunkFilter(int value1, int value2, int value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public void filter(IntChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final int checkValue = values.get(ii);
                if (checkValue == value1 || checkValue == value2 || checkValue == value3) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseThreeValueIntChunkFilter implements ChunkFilter.IntChunkFilter {
        private final int value1;
        private final int value2;
        private final int value3;

        private InverseThreeValueIntChunkFilter(int value1, int value2, int value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public void filter(IntChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final int checkValue = values.get(ii);
                if (!(checkValue == value1 || checkValue == value2 || checkValue == value3)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class MultiValueIntChunkFilter implements ChunkFilter.IntChunkFilter {
        private final TIntHashSet values;

        private MultiValueIntChunkFilter(int ... values) {
            this.values = new TIntHashSet(values);
        }

        @Override
        public void filter(IntChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final int checkValue = values.get(ii);
                if (this.values.contains(checkValue)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseMultiValueIntChunkFilter implements ChunkFilter.IntChunkFilter {
        private final TIntHashSet values;

        private InverseMultiValueIntChunkFilter(int ... values) {
            this.values = new TIntHashSet(values);
        }

        @Override
        public void filter(IntChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final int checkValue = values.get(ii);
                if (!this.values.contains(checkValue)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }
}