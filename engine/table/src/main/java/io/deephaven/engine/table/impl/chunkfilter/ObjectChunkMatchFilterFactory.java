//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;

public class ObjectChunkMatchFilterFactory {
    private ObjectChunkMatchFilterFactory() {} // static use only

    public static ChunkFilter.ObjectChunkFilter makeFilter(boolean invert, Object... values) {
        if (invert) {
            if (values.length == 1) {
                return new InverseSingleValueObjectChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new InverseTwoValueObjectChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new InverseThreeValueObjectChunkFilter(values[0], values[1], values[2]);
            }
            return new InverseMultiValueObjectChunkFilter(values);
        } else {
            if (values.length == 1) {
                return new SingleValueObjectChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new TwoValueObjectChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new ThreeValueObjectChunkFilter(values[0], values[1], values[2]);
            }
            return new MultiValueObjectChunkFilter(values);
        }
    }

    private static class SingleValueObjectChunkFilter implements ChunkFilter.ObjectChunkFilter<Object> {
        private final Object value;

        private SingleValueObjectChunkFilter(Object value) {
            this.value = value;
        }

        @Override
        public boolean matches(Object value) {
            return Objects.equals(value, this.value);
        }

        /*
         * NOTE: this method is identically repeated for every class below. This is to allow a single virtual lookup per
         * filtered chunk, rather than making virtual calls to matches() for every value in the chunk. This is a
         * performance optimization that helps at least on JVM <= 21. It may not be always necessary on newer JVMs.
         */
        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<Object, ? extends Values> objectChunk = values.asObjectChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseSingleValueObjectChunkFilter implements ChunkFilter.ObjectChunkFilter<Object> {
        private final Object value;

        private InverseSingleValueObjectChunkFilter(Object value) {
            this.value = value;
        }

        @Override
        public boolean matches(Object value) {
            return !Objects.equals(value, this.value);
        }

        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<Object, ? extends Values> objectChunk = values.asObjectChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class TwoValueObjectChunkFilter implements ChunkFilter.ObjectChunkFilter<Object> {
        private final Object value1;
        private final Object value2;

        private TwoValueObjectChunkFilter(Object value1, Object value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(Object value) {
            return Objects.equals(value, value1) || Objects.equals(value, value2);
        }

        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<Object, ? extends Values> objectChunk = values.asObjectChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseTwoValueObjectChunkFilter implements ChunkFilter.ObjectChunkFilter<Object> {
        private final Object value1;
        private final Object value2;

        private InverseTwoValueObjectChunkFilter(Object value1, Object value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(Object value) {
            return !(Objects.equals(value, value1) || Objects.equals(value, value2));
        }

        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<Object, ? extends Values> objectChunk = values.asObjectChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class ThreeValueObjectChunkFilter implements ChunkFilter.ObjectChunkFilter<Object> {
        private final Object value1;
        private final Object value2;
        private final Object value3;

        private ThreeValueObjectChunkFilter(Object value1, Object value2, Object value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(Object value) {
            return Objects.equals(value, value1) || Objects.equals(value, value2) || Objects.equals(value, value3);
        }

        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<Object, ? extends Values> objectChunk = values.asObjectChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseThreeValueObjectChunkFilter implements ChunkFilter.ObjectChunkFilter<Object> {
        private final Object value1;
        private final Object value2;
        private final Object value3;

        private InverseThreeValueObjectChunkFilter(Object value1, Object value2, Object value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(Object value) {
            return !Objects.equals(value, value1) && !Objects.equals(value, value2) && !Objects.equals(value, value3);
        }

        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<Object, ? extends Values> objectChunk = values.asObjectChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class MultiValueObjectChunkFilter implements ChunkFilter.ObjectChunkFilter<Object> {
        private final HashSet<?> values;

        private MultiValueObjectChunkFilter(Object... values) {
            this.values = new HashSet<>(Arrays.asList(values));
        }

        @Override
        public boolean matches(Object value) {
            return this.values.contains(value);
        }

        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<Object, ? extends Values> objectChunk = values.asObjectChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseMultiValueObjectChunkFilter implements ChunkFilter.ObjectChunkFilter<Object> {
        private final HashSet<?> values;

        private InverseMultiValueObjectChunkFilter(Object... values) {
            this.values = new HashSet<>(Arrays.asList(values));
        }

        @Override
        public boolean matches(Object value) {
            return !this.values.contains(value);
        }

        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<Object, ? extends Values> objectChunk = values.asObjectChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }
}
