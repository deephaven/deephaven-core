//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkMatchFilterFactory and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;
import io.deephaven.util.compare.LongComparisons;

/**
 * Creates chunk filters for long values.
 * <p>
 * The strategy is that for one, two, or three values we have specialized classes that will do the appropriate simple
 * equality check.
 * <p>
 * For more values, we use a trove set and check contains for each value in the chunk.
 */
public class LongChunkMatchFilterFactory {
    private LongChunkMatchFilterFactory() {} // static use only

    public static LongChunkFilter makeFilter(boolean invertMatch, long... values) {
        if (invertMatch) {
            if (values.length == 1) {
                return new InverseSingleValueLongChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new InverseTwoValueLongChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new InverseThreeValueLongChunkFilter(values[0], values[1], values[2]);
            }
            return new InverseMultiValueLongChunkFilter(values);
        } else {
            if (values.length == 1) {
                return new SingleValueLongChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new TwoValueLongChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new ThreeValueLongChunkFilter(values[0], values[1], values[2]);
            }
            return new MultiValueLongChunkFilter(values);
        }
    }

    private final static class SingleValueLongChunkFilter extends LongChunkFilter {
        private final long value;

        private SingleValueLongChunkFilter(long value) {
            this.value = value;
        }

        @Override
        public boolean matches(long value) {
            return LongComparisons.eq(value, this.value);
        }

        @Override
        public boolean overlaps(long inputLower, long inputUpper) {
            return LongComparisons.geq(value, inputLower) && LongComparisons.leq(value, inputUpper);
        }
    }

    private final static class InverseSingleValueLongChunkFilter extends LongChunkFilter {
        private final long value;

        private InverseSingleValueLongChunkFilter(long value) {
            this.value = value;
        }

        @Override
        public boolean matches(long value) {
            return !LongComparisons.eq(value, this.value);
        }

        @Override
        public boolean overlaps(long inputLower, long inputUpper) {
            // true if the range contains ANY long other than the excluded one
            return !(LongComparisons.eq(value, inputLower) && LongComparisons.eq(value, inputUpper));
        }
    }

    private final static class TwoValueLongChunkFilter extends LongChunkFilter {
        private final long value1;
        private final long value2;

        private TwoValueLongChunkFilter(long value1, long value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(long value) {
            return LongComparisons.eq(value, value1) || LongComparisons.eq(value, value2);
        }

        @Override
        public boolean overlaps(long inputLower, long inputUpper) {
            return (LongComparisons.geq(value1, inputLower) && LongComparisons.leq(value1, inputUpper)) ||
                    (LongComparisons.geq(value2, inputLower) && LongComparisons.leq(value2, inputUpper));
        }
    }

    private final static class InverseTwoValueLongChunkFilter extends LongChunkFilter {
        private final long value1;
        private final long value2;

        private InverseTwoValueLongChunkFilter(long value1, long value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(long value) {
            return !LongComparisons.eq(value, value1) && !LongComparisons.eq(value, value2);
        }

        @Override
        public boolean overlaps(long inputLower, long inputUpper) {
            for (long v = inputLower; v <= inputUpper; v++) { // long to avoid overflow issues
                final long value = (long) v;
                if (!LongComparisons.eq(value, value1) && !LongComparisons.eq(value, value2)) {
                    return true;
                }
            }
            return false;
        }
    }

    private final static class ThreeValueLongChunkFilter extends LongChunkFilter {
        private final long value1;
        private final long value2;
        private final long value3;

        private ThreeValueLongChunkFilter(long value1, long value2, long value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(long value) {
            return LongComparisons.eq(value, value1) ||
                    LongComparisons.eq(value, value2) ||
                    LongComparisons.eq(value, value3);
        }

        @Override
        public boolean overlaps(long inputLower, long inputUpper) {
            return (LongComparisons.geq(value1, inputLower) && LongComparisons.leq(value1, inputUpper)) ||
                    (LongComparisons.geq(value2, inputLower) && LongComparisons.leq(value2, inputUpper)) ||
                    (LongComparisons.geq(value3, inputLower) && LongComparisons.leq(value3, inputUpper));
        }
    }

    private final static class InverseThreeValueLongChunkFilter extends LongChunkFilter {
        private final long value1;
        private final long value2;
        private final long value3;

        private InverseThreeValueLongChunkFilter(long value1, long value2, long value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(long value) {
            return !LongComparisons.eq(value, value1) &&
                    !LongComparisons.eq(value, value2) &&
                    !LongComparisons.eq(value, value3);
        }

        @Override
        public boolean overlaps(long inputLower, long inputUpper) {
            for (long v = inputLower; v <= inputUpper; v++) {
                final long value = (long) v;
                if (!LongComparisons.eq(value, value1) && !LongComparisons.eq(value, value2)
                        && !LongComparisons.eq(value, value3)) {
                    return true;
                }
            }
            return false;
        }
    }

    private final static class MultiValueLongChunkFilter extends LongChunkFilter {
        private final TLongHashSet values;

        private MultiValueLongChunkFilter(long... values) {
            this.values = new TLongHashSet(values);
        }

        @Override
        public boolean matches(long value) {
            return this.values.contains(value);
        }

        @Override
        public boolean overlaps(long inputLower, long inputUpper) {
            final TLongIterator iterator = values.iterator();
            while (iterator.hasNext()) {
                final long value = iterator.next();
                if (LongComparisons.geq(value, inputLower) && LongComparisons.leq(value, inputUpper)) {
                    return true;
                }
            }
            return false;
        }
    }

    private final static class InverseMultiValueLongChunkFilter extends LongChunkFilter {
        private final TLongHashSet values;

        private InverseMultiValueLongChunkFilter(long... values) {
            this.values = new TLongHashSet(values);
        }

        @Override
        public boolean matches(long value) {
            return !this.values.contains(value);
        }

        @Override
        public boolean overlaps(long inputLower, long inputUpper) {
            for (long ci = inputLower; ci <= inputUpper; ci++) {
                if (!values.contains((long) ci)) {
                    return true;
                }
            }
            return false;
        }
    }
}
