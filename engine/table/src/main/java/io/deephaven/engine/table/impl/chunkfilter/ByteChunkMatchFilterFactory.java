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
            return ByteComparisons.geq(value, inputLower) && ByteComparisons.leq(value, inputUpper);
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
            // true if the range contains ANY byte other than the excluded one
            return !(ByteComparisons.eq(value, inputLower) && ByteComparisons.eq(value, inputUpper));
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
            return (ByteComparisons.geq(value1, inputLower) && ByteComparisons.leq(value1, inputUpper)) ||
                    (ByteComparisons.geq(value2, inputLower) && ByteComparisons.leq(value2, inputUpper));
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
            for (long v = inputLower; v <= inputUpper; v++) { // long to avoid overflow issues
                final byte value = (byte) v;
                if (!ByteComparisons.eq(value, value1) && !ByteComparisons.eq(value, value2)) {
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
            return (ByteComparisons.geq(value1, inputLower) && ByteComparisons.leq(value1, inputUpper)) ||
                    (ByteComparisons.geq(value2, inputLower) && ByteComparisons.leq(value2, inputUpper)) ||
                    (ByteComparisons.geq(value3, inputLower) && ByteComparisons.leq(value3, inputUpper));
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
            for (long v = inputLower; v <= inputUpper; v++) {
                final byte value = (byte) v;
                if (!ByteComparisons.eq(value, value1) && !ByteComparisons.eq(value, value2)
                        && !ByteComparisons.eq(value, value3)) {
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
                if (ByteComparisons.geq(value, inputLower) && ByteComparisons.leq(value, inputUpper)) {
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
            for (long ci = inputLower; ci <= inputUpper; ci++) {
                if (!values.contains((byte) ci)) {
                    return true;
                }
            }
            return false;
        }
    }
}
