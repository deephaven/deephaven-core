/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkMatchFilterFactory and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;
import gnu.trove.set.hash.TShortHashSet;

/**
 * Creates chunk filters for short values.
 *
 * The strategy is that for one, two, or three values we have specialized
 * classes that will do the appropriate simple equality check.
 *
 * For more values, we use a trove set and check contains for each value in the chunk.
 */
public class ShortChunkMatchFilterFactory {
    private ShortChunkMatchFilterFactory() {} // static use only

    public static ChunkFilter.ShortChunkFilter makeFilter(boolean invertMatch, short ... values) {
        if (invertMatch) {
            if (values.length == 1) {
                return new InverseSingleValueShortChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new InverseTwoValueShortChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new InverseThreeValueShortChunkFilter(values[0], values[1], values[2]);
            }
            return new InverseMultiValueShortChunkFilter(values);
        } else {
            if (values.length == 1) {
                return new SingleValueShortChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new TwoValueShortChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new ThreeValueShortChunkFilter(values[0], values[1], values[2]);
            }
            return new MultiValueShortChunkFilter(values);
        }
    }

    private static class SingleValueShortChunkFilter implements ChunkFilter.ShortChunkFilter {
        private final short value;

        private SingleValueShortChunkFilter(short value) {
            this.value = value;
        }

        @Override
        public void filter(ShortChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (values.get(ii) == value) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseSingleValueShortChunkFilter implements ChunkFilter.ShortChunkFilter {
        private final short value;

        private InverseSingleValueShortChunkFilter(short value) {
            this.value = value;
        }

        @Override
        public void filter(ShortChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (values.get(ii) != value) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class TwoValueShortChunkFilter implements ChunkFilter.ShortChunkFilter {
        private final short value1;
        private final short value2;

        private TwoValueShortChunkFilter(short value1, short value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public void filter(ShortChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final short checkValue = values.get(ii);
                if (checkValue == value1 || checkValue == value2) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseTwoValueShortChunkFilter implements ChunkFilter.ShortChunkFilter {
        private final short value1;
        private final short value2;

        private InverseTwoValueShortChunkFilter(short value1, short value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public void filter(ShortChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final short checkValue = values.get(ii);
                if (!(checkValue == value1 || checkValue == value2)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class ThreeValueShortChunkFilter implements ChunkFilter.ShortChunkFilter {
        private final short value1;
        private final short value2;
        private final short value3;

        private ThreeValueShortChunkFilter(short value1, short value2, short value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public void filter(ShortChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final short checkValue = values.get(ii);
                if (checkValue == value1 || checkValue == value2 || checkValue == value3) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseThreeValueShortChunkFilter implements ChunkFilter.ShortChunkFilter {
        private final short value1;
        private final short value2;
        private final short value3;

        private InverseThreeValueShortChunkFilter(short value1, short value2, short value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public void filter(ShortChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final short checkValue = values.get(ii);
                if (!(checkValue == value1 || checkValue == value2 || checkValue == value3)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class MultiValueShortChunkFilter implements ChunkFilter.ShortChunkFilter {
        private final TShortHashSet values;

        private MultiValueShortChunkFilter(short ... values) {
            this.values = new TShortHashSet(values);
        }

        @Override
        public void filter(ShortChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final short checkValue = values.get(ii);
                if (this.values.contains(checkValue)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseMultiValueShortChunkFilter implements ChunkFilter.ShortChunkFilter {
        private final TShortHashSet values;

        private InverseMultiValueShortChunkFilter(short ... values) {
            this.values = new TShortHashSet(values);
        }

        @Override
        public void filter(ShortChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final short checkValue = values.get(ii);
                if (!this.values.contains(checkValue)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }
}