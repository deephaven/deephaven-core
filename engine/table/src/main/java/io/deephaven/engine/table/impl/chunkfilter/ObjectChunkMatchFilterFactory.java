//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.util.compare.ObjectComparisons;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
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

        @Override
        public boolean overlaps(Object inputLower, Object inputUpper) {
            return ObjectComparisons.geq(value, inputLower) && ObjectComparisons.leq(value, inputUpper);
        }
    }

    /**
     * Checks if the given bound is excluded from the set of excluded values.
     */
    private static boolean boundIsExcluded(Object bound, Object... excluded) {
        for (Object ex : excluded) {
            if (Objects.equals(bound, ex)) {
                return true;
            }
        }
        return false;
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

        @Override
        public boolean overlaps(Object inputLower, Object inputUpper) {
            return !(ObjectComparisons.eq(inputLower, inputUpper) && ObjectComparisons.eq(inputLower, value));
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

        @Override
        public boolean overlaps(Object inputLower, Object inputUpper) {
            return (ObjectComparisons.geq(value1, inputLower) && ObjectComparisons.leq(value1, inputUpper)) ||
                    (ObjectComparisons.geq(value2, inputLower) && ObjectComparisons.leq(value2, inputUpper));
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

        @Override
        public boolean overlaps(Object inputLower, Object inputUpper) {
            // If either bound is NOT an excluded value, the range surely overlaps.
            if (!boundIsExcluded(inputLower, value1, value2) || !boundIsExcluded(inputUpper, value1, value2)) {
                return true;
            }
            throw new UncheckedDeephavenException("Failed to determine overlap for bounds: " +
                    inputLower + " and " + inputUpper + " with excluded values: " + value1 + ", " + value2);
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

        @Override
        public boolean overlaps(Object inputLower, Object inputUpper) {
            return (ObjectComparisons.geq(value1, inputLower) && ObjectComparisons.leq(value1, inputUpper)) ||
                    (ObjectComparisons.geq(value2, inputLower) && ObjectComparisons.leq(value2, inputUpper)) ||
                    (ObjectComparisons.geq(value3, inputLower) && ObjectComparisons.leq(value3, inputUpper));
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

        @Override
        public boolean overlaps(Object inputLower, Object inputUpper) {
            // If either bound is NOT an excluded value, the range surely overlaps.
            if (!boundIsExcluded(inputLower, value1, value2, value3) ||
                    !boundIsExcluded(inputUpper, value1, value2, value3)) {
                return true;
            }
            throw new UncheckedDeephavenException("Failed to determine overlap for bounds: " + inputLower + " and "
                    + inputUpper + " with excluded values: " + value1 + ", " + value2 + ", " + value3);
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

        @Override
        public boolean overlaps(Object inputLower, Object inputUpper) {
            final Iterator<?> iterator = values.iterator();
            while (iterator.hasNext()) {
                final Object value = iterator.next();
                if (ObjectComparisons.geq(value, inputLower) && ObjectComparisons.leq(value, inputUpper)) {
                    return true;
                }
            }
            return false;
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

        @Override
        public boolean overlaps(Object inputLower, Object inputUpper) {
            // If either bound is NOT an excluded value, the range surely overlaps.
            if (!values.contains(inputLower) || !values.contains(inputUpper)) {
                return true;
            }
            throw new UncheckedDeephavenException("Failed to determine overlap for bounds: " + inputLower + " and "
                    + inputUpper + " with excluded values: " + values);
        }
    }
}
