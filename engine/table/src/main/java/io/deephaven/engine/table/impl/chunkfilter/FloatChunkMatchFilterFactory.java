//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkMatchFilterFactory and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import gnu.trove.set.hash.TFloatHashSet;

/**
 * Creates chunk filters for float values.
 * <p>
 * The strategy is that for one, two, or three values we have specialized classes that will do the appropriate simple
 * equality check.
 * <p>
 * For more values, we use a trove set and check contains for each value in the chunk.
 */
public class FloatChunkMatchFilterFactory {
    private FloatChunkMatchFilterFactory() {} // static use only

    public static FloatChunkFilter makeFilter(boolean invertMatch, float... values) {
        if (invertMatch) {
            if (values.length == 1) {
                return new InverseSingleValueFloatChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new InverseTwoValueFloatChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new InverseThreeValueFloatChunkFilter(values[0], values[1], values[2]);
            }
            return new InverseMultiValueFloatChunkFilter(values);
        } else {
            if (values.length == 1) {
                return new SingleValueFloatChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new TwoValueFloatChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new ThreeValueFloatChunkFilter(values[0], values[1], values[2]);
            }
            return new MultiValueFloatChunkFilter(values);
        }
    }

    private final static class SingleValueFloatChunkFilter extends FloatChunkFilter {
        private final float value;

        private SingleValueFloatChunkFilter(float value) {
            this.value = value;
        }

        @Override
        public boolean matches(float value) {
            return value == this.value;
        }
    }

    private final static class InverseSingleValueFloatChunkFilter extends FloatChunkFilter {
        private final float value;

        private InverseSingleValueFloatChunkFilter(float value) {
            this.value = value;
        }

        @Override
        public boolean matches(float value) {
            return value != this.value;
        }
    }

    private final static class TwoValueFloatChunkFilter extends FloatChunkFilter {
        private final float value1;
        private final float value2;

        private TwoValueFloatChunkFilter(float value1, float value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(float value) {
            return value == value1 || value == value2;
        }
    }

    private final static class InverseTwoValueFloatChunkFilter extends FloatChunkFilter {
        private final float value1;
        private final float value2;

        private InverseTwoValueFloatChunkFilter(float value1, float value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(float value) {
            return value != value1 && value != value2;
        }
    }

    private final static class ThreeValueFloatChunkFilter extends FloatChunkFilter {
        private final float value1;
        private final float value2;
        private final float value3;

        private ThreeValueFloatChunkFilter(float value1, float value2, float value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(float value) {
            return value == value1 || value == value2 || value == value3;
        }
    }

    private final static class InverseThreeValueFloatChunkFilter extends FloatChunkFilter {
        private final float value1;
        private final float value2;
        private final float value3;

        private InverseThreeValueFloatChunkFilter(float value1, float value2, float value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(float value) {
            return value != value1 && value != value2 && value != value3;
        }
    }

    private final static class MultiValueFloatChunkFilter extends FloatChunkFilter {
        private final TFloatHashSet values;

        private MultiValueFloatChunkFilter(float... values) {
            this.values = new TFloatHashSet(values);
        }

        @Override
        public boolean matches(float value) {
            return this.values.contains(value);
        }
    }

    private final static class InverseMultiValueFloatChunkFilter extends FloatChunkFilter {
        private final TFloatHashSet values;

        private InverseMultiValueFloatChunkFilter(float... values) {
            this.values = new TFloatHashSet(values);
        }

        @Override
        public boolean matches(float value) {
            return !this.values.contains(value);
        }
    }
}
