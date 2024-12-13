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

        private boolean matches(Object value) {
            return Objects.equals(value, this.value);
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
            final ObjectChunk<Object, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = objectChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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

    private static class InverseSingleValueObjectChunkFilter implements ChunkFilter.ObjectChunkFilter<Object> {
        private final Object value;

        private InverseSingleValueObjectChunkFilter(Object value) {
            this.value = value;
        }

        private boolean matches(Object value) {
            return !Objects.equals(value, this.value);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<Object, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = objectChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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

    private static class TwoValueObjectChunkFilter implements ChunkFilter.ObjectChunkFilter<Object> {
        private final Object value1;
        private final Object value2;

        private TwoValueObjectChunkFilter(Object value1, Object value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        private boolean matches(Object value) {
            return Objects.equals(value, value1) || Objects.equals(value, value2);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<Object, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = objectChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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

    private static class InverseTwoValueObjectChunkFilter implements ChunkFilter.ObjectChunkFilter<Object> {
        private final Object value1;
        private final Object value2;

        private InverseTwoValueObjectChunkFilter(Object value1, Object value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        private boolean matches(Object value) {
            return !(Objects.equals(value, value1) || Objects.equals(value, value2));
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<Object, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = objectChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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

    private static class ThreeValueObjectChunkFilter implements ChunkFilter.ObjectChunkFilter<Object> {
        private final Object value1;
        private final Object value2;
        private final Object value3;

        private ThreeValueObjectChunkFilter(Object value1, Object value2, Object value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        private boolean matches(Object value) {
            return Objects.equals(value, value1) || Objects.equals(value, value2) || Objects.equals(value, value3);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<Object, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = objectChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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

    private static class InverseThreeValueObjectChunkFilter implements ChunkFilter.ObjectChunkFilter<Object> {
        private final Object value1;
        private final Object value2;
        private final Object value3;

        private InverseThreeValueObjectChunkFilter(Object value1, Object value2, Object value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        private boolean matches(Object value) {
            return !(Objects.equals(value, value1) || Objects.equals(value, value2) || Objects.equals(value, value3));
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<Object, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = objectChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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

    private static class MultiValueObjectChunkFilter implements ChunkFilter.ObjectChunkFilter<Object> {
        private final HashSet<?> values;

        private MultiValueObjectChunkFilter(Object... values) {
            this.values = new HashSet<>(Arrays.asList(values));
        }

        private boolean matches(Object value) {
            return this.values.contains(value);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<Object, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = objectChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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

    private static class InverseMultiValueObjectChunkFilter implements ChunkFilter.ObjectChunkFilter<Object> {
        private final HashSet<?> values;

        private InverseMultiValueObjectChunkFilter(Object... values) {
            this.values = new HashSet<>(Arrays.asList(values));
        }

        private boolean matches(Object value) {
            return !this.values.contains(value);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<Object, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = objectChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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
            final ObjectChunk<Object, ? extends Values> typedChunk = values.asObjectChunk();
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
