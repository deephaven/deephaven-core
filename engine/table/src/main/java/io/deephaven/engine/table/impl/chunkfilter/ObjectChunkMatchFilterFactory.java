//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;

public class ObjectChunkMatchFilterFactory {
    private ObjectChunkMatchFilterFactory() {} // static use only

    @SuppressWarnings("rawtypes")
    public static ObjectChunkFilter makeFilter(boolean invert, Object... values) {
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

    private final static class SingleValueObjectChunkFilter extends ObjectChunkFilter<Object> {
        private final Object value;

        private SingleValueObjectChunkFilter(Object value) {
            this.value = value;
        }

        @Override
        public boolean matches(Object value) {
            return Objects.equals(value, this.value);
        }
    }

    private final static class InverseSingleValueObjectChunkFilter extends ObjectChunkFilter<Object> {
        private final Object value;

        private InverseSingleValueObjectChunkFilter(Object value) {
            this.value = value;
        }

        @Override
        public boolean matches(Object value) {
            return !Objects.equals(value, this.value);
        }
    }

    private final static class TwoValueObjectChunkFilter extends ObjectChunkFilter<Object> {
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
    }

    private final static class InverseTwoValueObjectChunkFilter extends ObjectChunkFilter<Object> {
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
    }

    private final static class ThreeValueObjectChunkFilter extends ObjectChunkFilter<Object> {
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
    }

    private final static class InverseThreeValueObjectChunkFilter extends ObjectChunkFilter<Object> {
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
            return !(Objects.equals(value, value1) || Objects.equals(value, value2) || Objects.equals(value, value3));
        }
    }

    private final static class MultiValueObjectChunkFilter extends ObjectChunkFilter<Object> {
        private final HashSet<?> values;

        private MultiValueObjectChunkFilter(Object... values) {
            this.values = new HashSet<>(Arrays.asList(values));
        }

        @Override
        public boolean matches(Object value) {
            return this.values.contains(value);
        }
    }

    private final static class InverseMultiValueObjectChunkFilter extends ObjectChunkFilter<Object> {
        private final HashSet<?> values;

        private InverseMultiValueObjectChunkFilter(Object... values) {
            this.values = new HashSet<>(Arrays.asList(values));
        }

        @Override
        public boolean matches(Object value) {
            return !this.values.contains(value);
        }
    }
}
