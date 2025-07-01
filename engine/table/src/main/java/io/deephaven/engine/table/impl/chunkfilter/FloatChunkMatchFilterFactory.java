//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import gnu.trove.iterator.TFloatIterator;
import gnu.trove.set.hash.TFloatHashSet;
import io.deephaven.util.compare.FloatComparisons;

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
            return FloatComparisons.eq(value, this.value);
        }

        @Override
        public boolean overlaps(float inputLower, float inputUpper) {
            return FloatComparisons.leq(inputLower, value) && FloatComparisons.leq(value, inputUpper);
        }
    }

    private final static class InverseSingleValueFloatChunkFilter extends FloatChunkFilter {
        private final float value;

        private InverseSingleValueFloatChunkFilter(float value) {
            this.value = value;
        }

        @Override
        public boolean matches(float value) {
            return !FloatComparisons.eq(value, this.value);
        }

        @Override
        public boolean overlaps(float inputLower, float inputUpper) {
            // true if the range contains ANY float other than the excluded one
            return !(FloatComparisons.eq(value, inputLower) && FloatComparisons.eq(value, inputUpper));
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
            return FloatComparisons.eq(value, value1) || FloatComparisons.eq(value, value2);
        }

        @Override
        public boolean overlaps(float inputLower, float inputUpper) {
            return (FloatComparisons.leq(inputLower, value1) && FloatComparisons.leq(value1, inputUpper)) ||
                    (FloatComparisons.leq(inputLower, value2) && FloatComparisons.leq(value2, inputUpper));
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
            return !FloatComparisons.eq(value, value1) && !FloatComparisons.eq(value, value2);
        }

        @Override
        public boolean overlaps(float inputLower, float inputUpper) {
            final int maxSteps = 3; // two excluded values
            for (float value = inputLower, steps = 0; FloatComparisons.leq(value, inputUpper)
                    && steps < maxSteps; value = Math.nextAfter(value, Float.POSITIVE_INFINITY), ++steps) {
                if (!FloatComparisons.eq(value, value1) && !FloatComparisons.eq(value, value2)) {
                    return true;
                }
            }
            return false;
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
            return FloatComparisons.eq(value, value1) ||
                    FloatComparisons.eq(value, value2) ||
                    FloatComparisons.eq(value, value3);
        }

        @Override
        public boolean overlaps(float inputLower, float inputUpper) {
            return (FloatComparisons.leq(inputLower, value1) && FloatComparisons.leq(value1, inputUpper)) ||
                    (FloatComparisons.leq(inputLower, value2) && FloatComparisons.leq(value2, inputUpper)) ||
                    (FloatComparisons.leq(inputLower, value3) && FloatComparisons.leq(value3, inputUpper));
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
            return !FloatComparisons.eq(value, value1) &&
                    !FloatComparisons.eq(value, value2) &&
                    !FloatComparisons.eq(value, value3);
        }

        @Override
        public boolean overlaps(float inputLower, float inputUpper) {
            final int maxSteps = 4; // three excluded values
            for (float value = inputLower, steps = 0; FloatComparisons.leq(value, inputUpper)
                    && steps < maxSteps; value = Math.nextAfter(value, Float.POSITIVE_INFINITY), ++steps) {
                if (!FloatComparisons.eq(value, value1) &&
                        !FloatComparisons.eq(value, value2) &&
                        !FloatComparisons.eq(value, value3)) {
                    return true;
                }
            }
            return false;
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

        @Override
        public boolean overlaps(float inputLower, float inputUpper) {
            final TFloatIterator iterator = values.iterator();
            while (iterator.hasNext()) {
                final float value = iterator.next();
                if (FloatComparisons.leq(inputLower, value) && FloatComparisons.leq(value, inputUpper)) {
                    return true;
                }
            }
            return false;
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

        @Override
        public boolean overlaps(float inputLower, float inputUpper) {
            final int maxSteps = values.size() + 1;
            for (float value = inputLower, steps = 0; FloatComparisons.leq(value, inputUpper)
                    && steps < maxSteps; value = Math.nextAfter(value, Float.POSITIVE_INFINITY), ++steps) {
                if (!values.contains(value)) {
                    return true;
                }
            }
            return false;
        }
    }
}
