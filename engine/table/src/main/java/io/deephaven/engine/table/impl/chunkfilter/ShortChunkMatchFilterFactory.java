//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkMatchFilterFactory and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.set.hash.TShortHashSet;
import io.deephaven.util.compare.ShortComparisons;

/**
 * Creates chunk filters for short values.
 * <p>
 * The strategy is that for one, two, or three values we have specialized classes that will do the appropriate simple
 * equality check.
 * <p>
 * For more values, we use a trove set and check contains for each value in the chunk.
 */
public class ShortChunkMatchFilterFactory {
    private ShortChunkMatchFilterFactory() {} // static use only

    public static ShortChunkFilter makeFilter(boolean invertMatch, short... values) {
        if (invertMatch) {
            if (values.length == 1) {
                return new InverseSingleValueShortChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new InverseTwoValueShortChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new InverseThreeValueShortChunkFilter(values[0], values[1], values[2]);
            }
            return new InverseMultiValueShortChunkFilter(values);
        } else {
            if (values.length == 1) {
                return new SingleValueShortChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new TwoValueShortChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new ThreeValueShortChunkFilter(values[0], values[1], values[2]);
            }
            return new MultiValueShortChunkFilter(values);
        }
    }

    private final static class SingleValueShortChunkFilter extends ShortChunkFilter {
        private final short value;

        private SingleValueShortChunkFilter(short value) {
            this.value = value;
        }

        @Override
        public boolean matches(short value) {
            return ShortComparisons.eq(value, this.value);
        }

        @Override
        public boolean overlaps(short inputLower, short inputUpper) {
            return ShortComparisons.leq(inputLower, value) && ShortComparisons.leq(value, inputUpper);
        }
    }

    private final static class InverseSingleValueShortChunkFilter extends ShortChunkFilter {
        private final short value;

        private InverseSingleValueShortChunkFilter(short value) {
            this.value = value;
        }

        @Override
        public boolean matches(short value) {
            return !ShortComparisons.eq(value, this.value);
        }

        @Override
        public boolean overlaps(short inputLower, short inputUpper) {
            // Any interval wider than one point must include a short not equal to `value`, so we simply need to
            // check whether we have a single-point range [value,value] or not.
            return matches(inputLower) || matches(inputUpper);
        }
    }

    private final static class TwoValueShortChunkFilter extends ShortChunkFilter {
        private final short value1;
        private final short value2;

        private TwoValueShortChunkFilter(short value1, short value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(short value) {
            return ShortComparisons.eq(value, value1) || ShortComparisons.eq(value, value2);
        }

        @Override
        public boolean overlaps(short inputLower, short inputUpper) {
            return (ShortComparisons.leq(inputLower, value1) && ShortComparisons.leq(value1, inputUpper)) ||
                    (ShortComparisons.leq(inputLower, value2) && ShortComparisons.leq(value2, inputUpper));
        }
    }

    private final static class InverseTwoValueShortChunkFilter extends ShortChunkFilter {
        private final short value1;
        private final short value2;

        private InverseTwoValueShortChunkFilter(short value1, short value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(short value) {
            return !ShortComparisons.eq(value, value1) && !ShortComparisons.eq(value, value2);
        }

        @Override
        public boolean overlaps(short inputLower, short inputUpper) {
            // Iterate through the range from inputLower to inputUpper, checking for any value that matches the inverse
            // condition. We only need to check the first three shorts in the range because at max two shorts in
            // the range are excluded (value1 and value2).
            final int maxSteps = 3;
            for (long v = inputLower, steps = 0; v <= inputUpper && steps < maxSteps; v++, steps++) {
                if (matches((short) v)) {
                    return true;
                }
            }
            return false;
        }
    }

    private final static class ThreeValueShortChunkFilter extends ShortChunkFilter {
        private final short value1;
        private final short value2;
        private final short value3;

        private ThreeValueShortChunkFilter(short value1, short value2, short value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(short value) {
            return ShortComparisons.eq(value, value1) ||
                    ShortComparisons.eq(value, value2) ||
                    ShortComparisons.eq(value, value3);
        }

        @Override
        public boolean overlaps(short inputLower, short inputUpper) {
            return (ShortComparisons.leq(inputLower, value1) && ShortComparisons.leq(value1, inputUpper)) ||
                    (ShortComparisons.leq(inputLower, value2) && ShortComparisons.leq(value2, inputUpper)) ||
                    (ShortComparisons.leq(inputLower, value3) && ShortComparisons.leq(value3, inputUpper));
        }
    }

    private final static class InverseThreeValueShortChunkFilter extends ShortChunkFilter {
        private final short value1;
        private final short value2;
        private final short value3;

        private InverseThreeValueShortChunkFilter(short value1, short value2, short value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(short value) {
            return !ShortComparisons.eq(value, value1) &&
                    !ShortComparisons.eq(value, value2) &&
                    !ShortComparisons.eq(value, value3);
        }

        @Override
        public boolean overlaps(short inputLower, short inputUpper) {
            // Iterate through the range from inputLower to inputUpper, checking for any value that matches the inverse
            // condition. We only need to check the first four shorts in the range because at max three shorts
            // in the range are excluded (value1, value2, and value3).
            final int maxSteps = 4;
            for (long v = inputLower, steps = 0; v <= inputUpper && steps < maxSteps; v++, steps++) {
                if (matches((short) v)) {
                    return true;
                }
            }
            return false;
        }
    }

    private final static class MultiValueShortChunkFilter extends ShortChunkFilter {
        private final TShortHashSet values;

        private MultiValueShortChunkFilter(short... values) {
            this.values = new TShortHashSet(values);
        }

        @Override
        public boolean matches(short value) {
            return this.values.contains(value);
        }

        @Override
        public boolean overlaps(short inputLower, short inputUpper) {
            final TShortIterator iterator = values.iterator();
            while (iterator.hasNext()) {
                final short value = iterator.next();
                if (ShortComparisons.leq(inputLower, value) && ShortComparisons.leq(value, inputUpper)) {
                    return true;
                }
            }
            return false;
        }
    }

    private final static class InverseMultiValueShortChunkFilter extends ShortChunkFilter {
        private final TShortHashSet values;

        private InverseMultiValueShortChunkFilter(short... values) {
            this.values = new TShortHashSet(values);
        }

        @Override
        public boolean matches(short value) {
            return !this.values.contains(value);
        }

        @Override
        public boolean overlaps(short inputLower, short inputUpper) {
            // Iterate through the range from inputLower to inputUpper, checking for any value that matches the inverse
            // condition. We only need to check the first `values.size() + 1` shorts in the range because at max
            // `values.size()` shorts in the range are excluded.
            final int maxSteps = values.size() + 1;
            for (long v = inputLower, steps = 0; v <= inputUpper && steps < maxSteps; v++, steps++) {
                if (matches((short) v)) {
                    return true;
                }
            }
            return false;
        }
    }
}
