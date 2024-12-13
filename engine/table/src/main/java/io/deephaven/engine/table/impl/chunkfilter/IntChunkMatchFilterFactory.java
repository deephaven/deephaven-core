//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkMatchFilterFactory and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Creates chunk filters for int values.
 * <p>
 * The strategy is that for one, two, or three values we have specialized classes that will do the appropriate simple
 * equality check.
 * <p>
 * For more values, we use a trove set and check contains for each value in the chunk.
 */
public class IntChunkMatchFilterFactory {
    private IntChunkMatchFilterFactory() {} // static use only

    public static ChunkFilter.IntChunkFilter makeFilter(boolean invertMatch, int... values) {
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

        private boolean matches(int value) {
            return value == this.value;
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
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = typedChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(typedChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    continue; // already false, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 0 : 1;
            }
            return count;
        }

        @Override
        public int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    continue; // already true, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    private static class InverseSingleValueIntChunkFilter implements ChunkFilter.IntChunkFilter {
        private final int value;

        private InverseSingleValueIntChunkFilter(int value) {
            this.value = value;
        }

        private boolean matches(int value) {
            return value != this.value;
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = intChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(intChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    continue; // already false, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 0 : 1;
            }
            return count;
        }

        @Override
        public int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    continue; // already true, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    private static class TwoValueIntChunkFilter implements ChunkFilter.IntChunkFilter {
        private final int value1;
        private final int value2;

        private TwoValueIntChunkFilter(int value1, int value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        private boolean matches(int value) {
            return value == value1 || value == value2;
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = intChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(intChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    continue; // already false, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 0 : 1;
            }
            return count;
        }

        @Override
        public int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    continue; // already true, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    private static class InverseTwoValueIntChunkFilter implements ChunkFilter.IntChunkFilter {
        private final int value1;
        private final int value2;

        private InverseTwoValueIntChunkFilter(int value1, int value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        private boolean matches(int value) {
            return value != value1 && value != value2;
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = intChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(intChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    continue; // already false, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 0 : 1;
            }
            return count;
        }

        @Override
        public int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    continue; // already true, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
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

        private boolean matches(int value) {
            return value == value1 || value == value2 || value == value3;
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = intChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(intChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    continue; // already false, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 0 : 1;
            }
            return count;
        }

        @Override
        public int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    continue; // already true, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
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

        private boolean matches(int value) {
            return value != value1 && value != value2 && value != value3;
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = intChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(intChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    continue; // already false, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 0 : 1;
            }
            return count;
        }

        @Override
        public int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    continue; // already true, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    private static class MultiValueIntChunkFilter implements ChunkFilter.IntChunkFilter {
        private final TIntHashSet values;

        private MultiValueIntChunkFilter(int... values) {
            this.values = new TIntHashSet(values);
        }

        private boolean matches(int value) {
            return this.values.contains(value);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = intChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(intChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    continue; // already false, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 0 : 1;
            }
            return count;
        }

        @Override
        public int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    continue; // already true, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    private static class InverseMultiValueIntChunkFilter implements ChunkFilter.IntChunkFilter {
        private final TIntHashSet values;

        private InverseMultiValueIntChunkFilter(int... values) {
            this.values = new TIntHashSet(values);
        }

        private boolean matches(int value) {
            return !this.values.contains(value);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = intChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(intChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    continue; // already false, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 0 : 1;
            }
            return count;
        }

        @Override
        public int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> typedChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    continue; // already true, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }
}
