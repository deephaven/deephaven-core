package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;

public class ObjectChunkMatchFilterFactory {
    private ObjectChunkMatchFilterFactory() {} // static use only

    public static ChunkFilter.ObjectChunkFilter makeFilter(boolean invert, Object ... values) {
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
        public void filter(ObjectChunk<Object, ? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (Objects.equals(values.get(ii), value)) {
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
        public void filter(ObjectChunk<Object, ? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (!Objects.equals(values.get(ii), value)) {
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
        public void filter(ObjectChunk<Object, ? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final Object checkValue = values.get(ii);
                if (Objects.equals(checkValue, value1) || Objects.equals(checkValue, value2)) {
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
        public void filter(ObjectChunk<Object, ? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final Object checkValue = values.get(ii);
                if (!(Objects.equals(checkValue, value1) || Objects.equals(checkValue, value2))) {
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
        public void filter(ObjectChunk<Object, ? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final Object checkValue = values.get(ii);
                if (Objects.equals(checkValue, value1) || Objects.equals(checkValue, value2) || Objects.equals(checkValue, value3)) {
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
        public void filter(ObjectChunk<Object, ? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final Object checkValue = values.get(ii);
                if (!(Objects.equals(checkValue, value1) || Objects.equals(checkValue, value2) || Objects.equals(checkValue, value3))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class MultiValueObjectChunkFilter implements ChunkFilter.ObjectChunkFilter<Object> {
        private final HashSet<?> values;

        private MultiValueObjectChunkFilter(Object ... values) {
            this.values = new HashSet<>(Arrays.asList(values));
        }

        @Override
        public void filter(ObjectChunk<Object, ? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (this.values.contains(values.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseMultiValueObjectChunkFilter implements ChunkFilter.ObjectChunkFilter<Object> {
        private final HashSet<?> values;

        private InverseMultiValueObjectChunkFilter(Object ... values) {
            this.values = new HashSet<>(Arrays.asList(values));
        }

        @Override
        public void filter(ObjectChunk<Object, ? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (!this.values.contains(values.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }
}