//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkMatchFilterFactory and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import gnu.trove.set.hash.TShortHashSet;

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
            return value == this.value;
        }
    }

    private final static class InverseSingleValueShortChunkFilter extends ShortChunkFilter {
        private final short value;

        private InverseSingleValueShortChunkFilter(short value) {
            this.value = value;
        }

        @Override
        public boolean matches(short value) {
            return value != this.value;
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
            return value == value1 || value == value2;
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
            return value != value1 && value != value2;
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
            return value == value1 || value == value2 || value == value3;
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
            return value != value1 && value != value2 && value != value3;
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
    }
}
