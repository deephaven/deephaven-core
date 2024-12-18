//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import gnu.trove.set.hash.TCharHashSet;

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
            return value == this.value;
        }
    }

    private final static class InverseSingleValueCharChunkFilter extends CharChunkFilter {
        private final char value;

        private InverseSingleValueCharChunkFilter(char value) {
            this.value = value;
        }

        @Override
        public boolean matches(char value) {
            return value != this.value;
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
            return value == value1 || value == value2;
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
            return value != value1 && value != value2;
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
            return value == value1 || value == value2 || value == value3;
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
            return value != value1 && value != value2 && value != value3;
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
    }
}
