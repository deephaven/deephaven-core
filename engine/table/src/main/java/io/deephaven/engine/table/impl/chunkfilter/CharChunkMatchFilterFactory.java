//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import gnu.trove.iterator.TCharIterator;
import gnu.trove.set.hash.TCharHashSet;
import io.deephaven.util.compare.CharComparisons;

/**
 * Creates chunk filters for char values.
 * <p>
 * The strategy is that for one, two, or three values we have specialized classes that will do the appropriate simple
 * equality check.
 * <p>
 * For more values, we use a trove set and check contains for each value in the chunk.
 */
public class CharChunkMatchFilterFactory {
    private CharChunkMatchFilterFactory() {} // static use only

    public static CharChunkFilter makeFilter(boolean invertMatch, char... values) {
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

    private final static class SingleValueCharChunkFilter extends CharChunkFilter {
        private final char value;

        private SingleValueCharChunkFilter(char value) {
            this.value = value;
        }

        @Override
        public boolean matches(char value) {
            return CharComparisons.eq(value, this.value);
        }

        @Override
        public boolean overlaps(char inputLower, char inputUpper) {
            return CharComparisons.leq(inputLower, value) && CharComparisons.leq(value, inputUpper);
        }
    }

    private final static class InverseSingleValueCharChunkFilter extends CharChunkFilter {
        private final char value;

        private InverseSingleValueCharChunkFilter(char value) {
            this.value = value;
        }

        @Override
        public boolean matches(char value) {
            return !CharComparisons.eq(value, this.value);
        }

        @Override
        public boolean overlaps(char inputLower, char inputUpper) {
            // Any interval wider than one point must include a char not equal to `value`, so we simply need to
            // check whether we have a single-point range [value,value] or not.
            return matches(inputLower) || matches(inputUpper);
        }
    }

    private final static class TwoValueCharChunkFilter extends CharChunkFilter {
        private final char value1;
        private final char value2;

        private TwoValueCharChunkFilter(char value1, char value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(char value) {
            return CharComparisons.eq(value, value1) || CharComparisons.eq(value, value2);
        }

        @Override
        public boolean overlaps(char inputLower, char inputUpper) {
            return (CharComparisons.leq(inputLower, value1) && CharComparisons.leq(value1, inputUpper)) ||
                    (CharComparisons.leq(inputLower, value2) && CharComparisons.leq(value2, inputUpper));
        }
    }

    private final static class InverseTwoValueCharChunkFilter extends CharChunkFilter {
        private final char value1;
        private final char value2;

        private InverseTwoValueCharChunkFilter(char value1, char value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(char value) {
            return !CharComparisons.eq(value, value1) && !CharComparisons.eq(value, value2);
        }

        @Override
        public boolean overlaps(char inputLower, char inputUpper) {
            // Iterate through the range from inputLower to inputUpper, checking for any value that matches the inverse
            // condition. We only need to check the first three chars in the range because at max two chars in
            // the range are excluded (value1 and value2).
            final int maxSteps = 3;
            for (long v = inputLower, steps = 0; v <= inputUpper && steps < maxSteps; v++, steps++) {
                if (matches((char) v)) {
                    return true;
                }
            }
            return false;
        }
    }

    private final static class ThreeValueCharChunkFilter extends CharChunkFilter {
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
            return CharComparisons.eq(value, value1) ||
                    CharComparisons.eq(value, value2) ||
                    CharComparisons.eq(value, value3);
        }

        @Override
        public boolean overlaps(char inputLower, char inputUpper) {
            return (CharComparisons.leq(inputLower, value1) && CharComparisons.leq(value1, inputUpper)) ||
                    (CharComparisons.leq(inputLower, value2) && CharComparisons.leq(value2, inputUpper)) ||
                    (CharComparisons.leq(inputLower, value3) && CharComparisons.leq(value3, inputUpper));
        }
    }

    private final static class InverseThreeValueCharChunkFilter extends CharChunkFilter {
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
            return !CharComparisons.eq(value, value1) &&
                    !CharComparisons.eq(value, value2) &&
                    !CharComparisons.eq(value, value3);
        }

        @Override
        public boolean overlaps(char inputLower, char inputUpper) {
            // Iterate through the range from inputLower to inputUpper, checking for any value that matches the inverse
            // condition. We only need to check the first four chars in the range because at max three chars
            // in the range are excluded (value1, value2, and value3).
            final int maxSteps = 4;
            for (long v = inputLower, steps = 0; v <= inputUpper && steps < maxSteps; v++, steps++) {
                if (matches((char) v)) {
                    return true;
                }
            }
            return false;
        }
    }

    private final static class MultiValueCharChunkFilter extends CharChunkFilter {
        private final TCharHashSet values;

        private MultiValueCharChunkFilter(char... values) {
            this.values = new TCharHashSet(values);
        }

        @Override
        public boolean matches(char value) {
            return this.values.contains(value);
        }

        @Override
        public boolean overlaps(char inputLower, char inputUpper) {
            final TCharIterator iterator = values.iterator();
            while (iterator.hasNext()) {
                final char value = iterator.next();
                if (CharComparisons.leq(inputLower, value) && CharComparisons.leq(value, inputUpper)) {
                    return true;
                }
            }
            return false;
        }
    }

    private final static class InverseMultiValueCharChunkFilter extends CharChunkFilter {
        private final TCharHashSet values;

        private InverseMultiValueCharChunkFilter(char... values) {
            this.values = new TCharHashSet(values);
        }

        @Override
        public boolean matches(char value) {
            return !this.values.contains(value);
        }

        @Override
        public boolean overlaps(char inputLower, char inputUpper) {
            // Iterate through the range from inputLower to inputUpper, checking for any value that matches the inverse
            // condition. We only need to check the first `values.size() + 1` chars in the range because at max
            // `values.size()` chars in the range are excluded.
            final int maxSteps = values.size() + 1;
            for (long v = inputLower, steps = 0; v <= inputUpper && steps < maxSteps; v++, steps++) {
                if (matches((char) v)) {
                    return true;
                }
            }
            return false;
        }
    }
}
