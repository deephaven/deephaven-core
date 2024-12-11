//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;
import gnu.trove.set.hash.TCharHashSet;

/**
 * Creates chunk filters for char values.
 *
 * The strategy is that for one, two, or three values we have specialized classes that will do the appropriate simple
 * equality check.
 *
 * For more values, we use a trove set and check contains for each value in the chunk.
 */
public class CharChunkMatchFilterFactory {
    private CharChunkMatchFilterFactory() {} // static use only

    public static ChunkFilter.CharChunkFilter makeFilter(boolean invertMatch, char... values) {
        if (invertMatch) {
            if (values.length == 1) {
                return new InverseSingleValueCharChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new InverseTwoValueCharChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new InverseThreeValueCharChunkFilter(values[0], values[1], values[2]);
            }
            return new InverseMultiValueCharChunkFilter(values);
        } else {
            if (values.length == 1) {
                return new SingleValueCharChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new TwoValueCharChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new ThreeValueCharChunkFilter(values[0], values[1], values[2]);
            }
            return new MultiValueCharChunkFilter(values);
        }
    }

    private static class SingleValueCharChunkFilter implements ChunkFilter.CharChunkFilter {
        private final char value;

        private SingleValueCharChunkFilter(char value) {
            this.value = value;
        }

        @Override
        public boolean matches(char value) {
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
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(charChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(charChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    private static class InverseSingleValueCharChunkFilter implements ChunkFilter.CharChunkFilter {
        private final char value;

        private InverseSingleValueCharChunkFilter(char value) {
            this.value = value;
        }

        @Override
        public boolean matches(char value) {
            return value != this.value;
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(charChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(charChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    private static class TwoValueCharChunkFilter implements ChunkFilter.CharChunkFilter {
        private final char value1;
        private final char value2;

        private TwoValueCharChunkFilter(char value1, char value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(char value) {
            return value == value1 || value == value2;
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(charChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(charChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    private static class InverseTwoValueCharChunkFilter implements ChunkFilter.CharChunkFilter {
        private final char value1;
        private final char value2;

        private InverseTwoValueCharChunkFilter(char value1, char value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(char value) {
            return value != value1 && value != value2;
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(charChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(charChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    private static class ThreeValueCharChunkFilter implements ChunkFilter.CharChunkFilter {
        private final char value1;
        private final char value2;
        private final char value3;

        private ThreeValueCharChunkFilter(char value1, char value2, char value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(char value) {
            return value == value1 || value == value2 || value == value3;
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(charChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(charChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    private static class InverseThreeValueCharChunkFilter implements ChunkFilter.CharChunkFilter {
        private final char value1;
        private final char value2;
        private final char value3;

        private InverseThreeValueCharChunkFilter(char value1, char value2, char value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(char value) {
            return value != value1 && value != value2 && value != value3;
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(charChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(charChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    private static class MultiValueCharChunkFilter implements ChunkFilter.CharChunkFilter {
        private final TCharHashSet values;

        private MultiValueCharChunkFilter(char... values) {
            this.values = new TCharHashSet(values);
        }

        @Override
        public boolean matches(char value) {
            return this.values.contains(value);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(charChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(charChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    private static class InverseMultiValueCharChunkFilter implements ChunkFilter.CharChunkFilter {
        private final TCharHashSet values;

        private InverseMultiValueCharChunkFilter(char... values) {
            this.values = new TCharHashSet(values);
        }

        @Override
        public boolean matches(char value) {
            return !this.values.contains(value);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(charChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(charChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }
}
