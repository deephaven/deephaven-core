//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkMatchFilterFactory and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import gnu.trove.iterator.TByteIterator;
import gnu.trove.set.hash.TByteHashSet;
import io.deephaven.util.compare.ByteComparisons;

/**
 * Creates chunk filters for byte values.
 * <p>
 * The strategy is that for one, two, or three values we have specialized classes that will do the appropriate simple
 * equality check.
 * <p>
 * For more values, we use a trove set and check contains for each value in the chunk.
 */
public class ByteChunkMatchFilterFactory {
    private ByteChunkMatchFilterFactory() {} // static use only

    public static ByteChunkFilter makeFilter(boolean invertMatch, byte... values) {
        if (invertMatch) {
            if (values.length == 1) {
                return new InverseSingleValueByteChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new InverseTwoValueByteChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new InverseThreeValueByteChunkFilter(values[0], values[1], values[2]);
            }
            return new InverseMultiValueByteChunkFilter(values);
        } else {
            if (values.length == 1) {
                return new SingleValueByteChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new TwoValueByteChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new ThreeValueByteChunkFilter(values[0], values[1], values[2]);
            }
            return new MultiValueByteChunkFilter(values);
        }
    }

    private final static class SingleValueByteChunkFilter extends ByteChunkFilter {
        private final byte value;

        private SingleValueByteChunkFilter(byte value) {
            this.value = value;
        }

        @Override
        public boolean matches(byte value) {
            return ByteComparisons.eq(value, this.value);
        }

        @Override
        public boolean overlaps(byte inputLower, byte inputUpper) {
            return ByteComparisons.leq(inputLower, value) && ByteComparisons.leq(value, inputUpper);
        }
    }

    private final static class InverseSingleValueByteChunkFilter extends ByteChunkFilter {
        private final byte value;

        private InverseSingleValueByteChunkFilter(byte value) {
            this.value = value;
        }

        @Override
        public boolean matches(byte value) {
            return !ByteComparisons.eq(value, this.value);
        }

        @Override
        public boolean overlaps(byte inputLower, byte inputUpper) {
            // Any interval wider than one point must include a byte not equal to `value`, so we simply need to
            // check whether we have a single-point range [value,value] or not.
            return matches(inputLower) || matches(inputUpper);
        }
    }

    private final static class TwoValueByteChunkFilter extends ByteChunkFilter {
        private final byte value1;
        private final byte value2;

        private TwoValueByteChunkFilter(byte value1, byte value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(byte value) {
            return ByteComparisons.eq(value, value1) || ByteComparisons.eq(value, value2);
        }

        @Override
        public boolean overlaps(byte inputLower, byte inputUpper) {
            return (ByteComparisons.leq(inputLower, value1) && ByteComparisons.leq(value1, inputUpper)) ||
                    (ByteComparisons.leq(inputLower, value2) && ByteComparisons.leq(value2, inputUpper));
        }
    }

    private final static class InverseTwoValueByteChunkFilter extends ByteChunkFilter {
        private final byte value1;
        private final byte value2;

        private InverseTwoValueByteChunkFilter(byte value1, byte value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(byte value) {
            return !ByteComparisons.eq(value, value1) && !ByteComparisons.eq(value, value2);
        }

        @Override
        public boolean overlaps(byte inputLower, byte inputUpper) {
            // Iterate through the range from inputLower to inputUpper, checking for any value that matches the inverse
            // condition. We only need to check the first three bytes in the range because at max two bytes in
            // the range are excluded (value1 and value2).
            final int maxSteps = 3;
            for (long v = inputLower, steps = 0; v <= inputUpper && steps < maxSteps; v++, steps++) {
                if (matches((byte) v)) {
                    return true;
                }
            }
            return false;
        }
    }

    private final static class ThreeValueByteChunkFilter extends ByteChunkFilter {
        private final byte value1;
        private final byte value2;
        private final byte value3;

        private ThreeValueByteChunkFilter(byte value1, byte value2, byte value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(byte value) {
            return ByteComparisons.eq(value, value1) ||
                    ByteComparisons.eq(value, value2) ||
                    ByteComparisons.eq(value, value3);
        }

        @Override
        public boolean overlaps(byte inputLower, byte inputUpper) {
            return (ByteComparisons.leq(inputLower, value1) && ByteComparisons.leq(value1, inputUpper)) ||
                    (ByteComparisons.leq(inputLower, value2) && ByteComparisons.leq(value2, inputUpper)) ||
                    (ByteComparisons.leq(inputLower, value3) && ByteComparisons.leq(value3, inputUpper));
        }
    }

    private final static class InverseThreeValueByteChunkFilter extends ByteChunkFilter {
        private final byte value1;
        private final byte value2;
        private final byte value3;

        private InverseThreeValueByteChunkFilter(byte value1, byte value2, byte value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(byte value) {
            return !ByteComparisons.eq(value, value1) &&
                    !ByteComparisons.eq(value, value2) &&
                    !ByteComparisons.eq(value, value3);
        }

        @Override
        public boolean overlaps(byte inputLower, byte inputUpper) {
            // Iterate through the range from inputLower to inputUpper, checking for any value that matches the inverse
            // condition. We only need to check the first four bytes in the range because at max three bytes
            // in the range are excluded (value1, value2, and value3).
            final int maxSteps = 4;
            for (long v = inputLower, steps = 0; v <= inputUpper && steps < maxSteps; v++, steps++) {
                if (matches((byte) v)) {
                    return true;
                }
            }
            return false;
        }
    }

    private final static class MultiValueByteChunkFilter extends ByteChunkFilter {
        private final TByteHashSet values;

        private MultiValueByteChunkFilter(byte... values) {
            this.values = new TByteHashSet(values);
        }

        @Override
        public boolean matches(byte value) {
            return this.values.contains(value);
        }

        @Override
        public boolean overlaps(byte inputLower, byte inputUpper) {
            final TByteIterator iterator = values.iterator();
            while (iterator.hasNext()) {
                final byte value = iterator.next();
                if (ByteComparisons.leq(inputLower, value) && ByteComparisons.leq(value, inputUpper)) {
                    return true;
                }
            }
            return false;
        }
    }

    private final static class InverseMultiValueByteChunkFilter extends ByteChunkFilter {
        private final TByteHashSet values;

        private InverseMultiValueByteChunkFilter(byte... values) {
            this.values = new TByteHashSet(values);
        }

        @Override
        public boolean matches(byte value) {
            return !this.values.contains(value);
        }

        @Override
        public boolean overlaps(byte inputLower, byte inputUpper) {
            // Iterate through the range from inputLower to inputUpper, checking for any value that matches the inverse
            // condition. We only need to check the first `values.size() + 1` bytes in the range because at max
            // `values.size()` bytes in the range are excluded.
            final int maxSteps = values.size() + 1;
            for (long v = inputLower, steps = 0; v <= inputUpper && steps < maxSteps; v++, steps++) {
                if (matches((byte) v)) {
                    return true;
                }
            }
            return false;
        }
    }
}
