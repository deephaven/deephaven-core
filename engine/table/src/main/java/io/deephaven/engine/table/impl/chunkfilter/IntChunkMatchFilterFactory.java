//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkMatchFilterFactory and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import gnu.trove.set.hash.TIntHashSet;

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
            return value == this.value;
        }
    }

    private final static class InverseSingleValueIntChunkFilter extends IntChunkFilter {
        private final int value;

        private InverseSingleValueIntChunkFilter(int value) {
            this.value = value;
        }

        @Override
        public boolean matches(int value) {
            return value != this.value;
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
            return value == value1 || value == value2;
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
            return value != value1 && value != value2;
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
            return value == value1 || value == value2 || value == value3;
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
            return value != value1 && value != value2 && value != value3;
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
    }
}
