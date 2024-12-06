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
import gnu.trove.set.hash.TByteHashSet;

/**
 * Creates chunk filters for byte values.
 *
 * The strategy is that for one, two, or three values we have specialized classes that will do the appropriate simple
 * equality check.
 *
 * For more values, we use a trove set and check contains for each value in the chunk.
 */
public class ByteChunkMatchFilterFactory {
    private ByteChunkMatchFilterFactory() {} // static use only

    public static ChunkFilter.ByteChunkFilter makeFilter(boolean invertMatch, byte... values) {
        if (invertMatch) {
            if (values.length == 1) {
                return new InverseSingleValueByteChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new InverseTwoValueByteChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new InverseThreeValueByteChunkFilter(values[0], values[1], values[2]);
            }
            return new InverseMultiValueByteChunkFilter(values);
        } else {
            if (values.length == 1) {
                return new SingleValueByteChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new TwoValueByteChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new ThreeValueByteChunkFilter(values[0], values[1], values[2]);
            }
            return new MultiValueByteChunkFilter(values);
        }
    }

    private static class SingleValueByteChunkFilter implements ChunkFilter.ByteChunkFilter {
        private final byte value;

        private SingleValueByteChunkFilter(byte value) {
            this.value = value;
        }

        @Override
        public boolean matches(byte value) {
            return value == this.value;
        }

        /*
         * NOTE: this method is identically repeated for every class below. This is to allow a single virtual lookup
         * per filtered chunk, rather than making virtual calls to matches() for every value in the chunk. This
         * is a performance optimization that helps at least on JVM <= 21. It may not be always necessary on newer JVMs.
         */
        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ByteChunk<? extends Values> byteChunk = values.asByteChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(byteChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseSingleValueByteChunkFilter implements ChunkFilter.ByteChunkFilter {
        private final byte value;

        private InverseSingleValueByteChunkFilter(byte value) {
            this.value = value;
        }

        @Override
        public boolean matches(byte value) {
            return value != this.value;
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ByteChunk<? extends Values> byteChunk = values.asByteChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(byteChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class TwoValueByteChunkFilter implements ChunkFilter.ByteChunkFilter {
        private final byte value1;
        private final byte value2;

        private TwoValueByteChunkFilter(byte value1, byte value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(byte value) {
            return value == value1 || value == value2;
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ByteChunk<? extends Values> byteChunk = values.asByteChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(byteChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseTwoValueByteChunkFilter implements ChunkFilter.ByteChunkFilter {
        private final byte value1;
        private final byte value2;

        private InverseTwoValueByteChunkFilter(byte value1, byte value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(byte value) {
            return value != value1 && value != value2;
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ByteChunk<? extends Values> byteChunk = values.asByteChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(byteChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class ThreeValueByteChunkFilter implements ChunkFilter.ByteChunkFilter {
        private final byte value1;
        private final byte value2;
        private final byte value3;

        private ThreeValueByteChunkFilter(byte value1, byte value2, byte value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(byte value) {
            return value == value1 || value == value2 || value == value3;
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ByteChunk<? extends Values> byteChunk = values.asByteChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(byteChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseThreeValueByteChunkFilter implements ChunkFilter.ByteChunkFilter {
        private final byte value1;
        private final byte value2;
        private final byte value3;

        private InverseThreeValueByteChunkFilter(byte value1, byte value2, byte value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(byte value) {
            return value != value1 && value != value2 && value != value3;
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ByteChunk<? extends Values> byteChunk = values.asByteChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(byteChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class MultiValueByteChunkFilter implements ChunkFilter.ByteChunkFilter {
        private final TByteHashSet values;

        private MultiValueByteChunkFilter(byte... values) {
            this.values = new TByteHashSet(values);
        }

        @Override
        public boolean matches(byte value) {
            return values.contains(value);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ByteChunk<? extends Values> byteChunk = values.asByteChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(byteChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseMultiValueByteChunkFilter implements ChunkFilter.ByteChunkFilter {
        private final TByteHashSet values;

        private InverseMultiValueByteChunkFilter(byte... values) {
            this.values = new TByteHashSet(values);
        }

        @Override
        public boolean matches(byte value) {
            return !values.contains(value);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ByteChunk<? extends Values> byteChunk = values.asByteChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(byteChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }
}
