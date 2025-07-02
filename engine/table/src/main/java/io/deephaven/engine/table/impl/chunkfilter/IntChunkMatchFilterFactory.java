//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkMatchFilterFactory and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.hash.TIntHashSet;
import io.deephaven.util.compare.IntComparisons;

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

    public static IntChunkFilter makeFilter(boolean invertMatch, int... values) {
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

    private final static class SingleValueIntChunkFilter extends IntChunkFilter {
        private final int value;

        private SingleValueIntChunkFilter(int value) {
            this.value = value;
        }

        @Override
        public boolean matches(int value) {
            return IntComparisons.eq(value, this.value);
        }

        @Override
        public boolean overlaps(int inputLower, int inputUpper) {
            return IntComparisons.leq(inputLower, value) && IntComparisons.leq(value, inputUpper);
        }
    }

    private final static class InverseSingleValueIntChunkFilter extends IntChunkFilter {
        private final int value;

        private InverseSingleValueIntChunkFilter(int value) {
            this.value = value;
        }

        @Override
        public boolean matches(int value) {
            return !IntComparisons.eq(value, this.value);
        }

        @Override
        public boolean overlaps(int inputLower, int inputUpper) {
            // Any interval wider than one point must include a int not equal to `value`, so we simply need to
            // check whether we have a single-point range [value,value] or not.
            return matches(inputLower) || matches(inputUpper);
        }
    }

    private final static class TwoValueIntChunkFilter extends IntChunkFilter {
        private final int value1;
        private final int value2;

        private TwoValueIntChunkFilter(int value1, int value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(int value) {
            return IntComparisons.eq(value, value1) || IntComparisons.eq(value, value2);
        }

        @Override
        public boolean overlaps(int inputLower, int inputUpper) {
            return (IntComparisons.leq(inputLower, value1) && IntComparisons.leq(value1, inputUpper)) ||
                    (IntComparisons.leq(inputLower, value2) && IntComparisons.leq(value2, inputUpper));
        }
    }

    private final static class InverseTwoValueIntChunkFilter extends IntChunkFilter {
        private final int value1;
        private final int value2;

        private InverseTwoValueIntChunkFilter(int value1, int value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(int value) {
            return !IntComparisons.eq(value, value1) && !IntComparisons.eq(value, value2);
        }

        @Override
        public boolean overlaps(int inputLower, int inputUpper) {
            // Iterate through the range from inputLower to inputUpper, checking for any value that matches the inverse
            // condition. We only need to check the first three ints in the range because at max two ints in
            // the range are excluded (value1 and value2).
            final int maxSteps = 3;
            for (long v = inputLower, steps = 0; v <= inputUpper && steps < maxSteps; v++, steps++) {
                if (matches((int) v)) {
                    return true;
                }
            }
            return false;
        }
    }

    private final static class ThreeValueIntChunkFilter extends IntChunkFilter {
        private final int value1;
        private final int value2;
        private final int value3;

        private ThreeValueIntChunkFilter(int value1, int value2, int value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(int value) {
            return IntComparisons.eq(value, value1) ||
                    IntComparisons.eq(value, value2) ||
                    IntComparisons.eq(value, value3);
        }

        @Override
        public boolean overlaps(int inputLower, int inputUpper) {
            return (IntComparisons.leq(inputLower, value1) && IntComparisons.leq(value1, inputUpper)) ||
                    (IntComparisons.leq(inputLower, value2) && IntComparisons.leq(value2, inputUpper)) ||
                    (IntComparisons.leq(inputLower, value3) && IntComparisons.leq(value3, inputUpper));
        }
    }

    private final static class InverseThreeValueIntChunkFilter extends IntChunkFilter {
        private final int value1;
        private final int value2;
        private final int value3;

        private InverseThreeValueIntChunkFilter(int value1, int value2, int value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(int value) {
            return !IntComparisons.eq(value, value1) &&
                    !IntComparisons.eq(value, value2) &&
                    !IntComparisons.eq(value, value3);
        }

        @Override
        public boolean overlaps(int inputLower, int inputUpper) {
            // Iterate through the range from inputLower to inputUpper, checking for any value that matches the inverse
            // condition. We only need to check the first four ints in the range because at max three ints
            // in the range are excluded (value1, value2, and value3).
            final int maxSteps = 4;
            for (long v = inputLower, steps = 0; v <= inputUpper && steps < maxSteps; v++, steps++) {
                if (matches((int) v)) {
                    return true;
                }
            }
            return false;
        }
    }

    private final static class MultiValueIntChunkFilter extends IntChunkFilter {
        private final TIntHashSet values;

        private MultiValueIntChunkFilter(int... values) {
            this.values = new TIntHashSet(values);
        }

        @Override
        public boolean matches(int value) {
            return this.values.contains(value);
        }

        @Override
        public boolean overlaps(int inputLower, int inputUpper) {
            final TIntIterator iterator = values.iterator();
            while (iterator.hasNext()) {
                final int value = iterator.next();
                if (IntComparisons.leq(inputLower, value) && IntComparisons.leq(value, inputUpper)) {
                    return true;
                }
            }
            return false;
        }
    }

    private final static class InverseMultiValueIntChunkFilter extends IntChunkFilter {
        private final TIntHashSet values;

        private InverseMultiValueIntChunkFilter(int... values) {
            this.values = new TIntHashSet(values);
        }

        @Override
        public boolean matches(int value) {
            return !this.values.contains(value);
        }

        @Override
        public boolean overlaps(int inputLower, int inputUpper) {
            // Iterate through the range from inputLower to inputUpper, checking for any value that matches the inverse
            // condition. We only need to check the first `values.size() + 1` ints in the range because at max
            // `values.size()` ints in the range are excluded.
            final int maxSteps = values.size() + 1;
            for (long v = inputLower, steps = 0; v <= inputUpper && steps < maxSteps; v++, steps++) {
                if (matches((int) v)) {
                    return true;
                }
            }
            return false;
        }
    }
}
