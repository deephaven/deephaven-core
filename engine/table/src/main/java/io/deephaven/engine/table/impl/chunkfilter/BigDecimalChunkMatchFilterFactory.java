//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.engine.table.MatchOptions;

import java.math.BigDecimal;
import java.util.Arrays;

/**
 * Match filters for {@link BigDecimal} columns, which match by {@link BigDecimal#compareTo(BigDecimal)}, as the query
 * language's {@code ==} does, rather than by {@link BigDecimal#equals(Object)}: {@code 5.0} matches {@code 5} and
 * {@code 5.00}, whatever their scales. {@code null} matches {@code null} and nothing else.
 */
class BigDecimalChunkMatchFilterFactory {
    private BigDecimalChunkMatchFilterFactory() {} // static use only

    private static boolean eq(final BigDecimal a, final BigDecimal b) {
        return a == null ? b == null : b != null && a.compareTo(b) == 0;
    }

    /**
     * Create a filter for the provided values. Assumes that every value is a {@link BigDecimal} or {@code null}.
     */
    @SuppressWarnings("rawtypes")
    static ObjectChunkFilter makeFilter(final MatchOptions matchOptions, final Object... values) {
        if (matchOptions.inverted()) {
            if (values.length == 1) {
                return new InverseSingleValueBigDecimalChunkFilter((BigDecimal) values[0]);
            }
            if (values.length == 2) {
                return new InverseTwoValueBigDecimalChunkFilter((BigDecimal) values[0], (BigDecimal) values[1]);
            }
            if (values.length == 3) {
                return new InverseThreeValueBigDecimalChunkFilter((BigDecimal) values[0], (BigDecimal) values[1],
                        (BigDecimal) values[2]);
            }
            return new InverseMultiValueBigDecimalChunkFilter(values);
        } else {
            if (values.length == 1) {
                return new SingleValueBigDecimalChunkFilter((BigDecimal) values[0]);
            }
            if (values.length == 2) {
                return new TwoValueBigDecimalChunkFilter((BigDecimal) values[0], (BigDecimal) values[1]);
            }
            if (values.length == 3) {
                return new ThreeValueBigDecimalChunkFilter((BigDecimal) values[0], (BigDecimal) values[1],
                        (BigDecimal) values[2]);
            }
            return new MultiValueBigDecimalChunkFilter(values);
        }
    }

    private final static class SingleValueBigDecimalChunkFilter extends ObjectChunkFilter<BigDecimal> {
        private final BigDecimal value;

        private SingleValueBigDecimalChunkFilter(BigDecimal value) {
            this.value = value;
        }

        @Override
        public boolean matches(BigDecimal value) {
            return eq(value, this.value);
        }
    }

    private final static class InverseSingleValueBigDecimalChunkFilter extends ObjectChunkFilter<BigDecimal> {
        private final BigDecimal value;

        private InverseSingleValueBigDecimalChunkFilter(BigDecimal value) {
            this.value = value;
        }

        @Override
        public boolean matches(BigDecimal value) {
            return !eq(value, this.value);
        }
    }

    private final static class TwoValueBigDecimalChunkFilter extends ObjectChunkFilter<BigDecimal> {
        private final BigDecimal value1;
        private final BigDecimal value2;

        private TwoValueBigDecimalChunkFilter(BigDecimal value1, BigDecimal value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(BigDecimal value) {
            return eq(value, value1) || eq(value, value2);
        }
    }

    private final static class InverseTwoValueBigDecimalChunkFilter extends ObjectChunkFilter<BigDecimal> {
        private final BigDecimal value1;
        private final BigDecimal value2;

        private InverseTwoValueBigDecimalChunkFilter(BigDecimal value1, BigDecimal value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(BigDecimal value) {
            return !(eq(value, value1) || eq(value, value2));
        }
    }

    private final static class ThreeValueBigDecimalChunkFilter extends ObjectChunkFilter<BigDecimal> {
        private final BigDecimal value1;
        private final BigDecimal value2;
        private final BigDecimal value3;

        private ThreeValueBigDecimalChunkFilter(BigDecimal value1, BigDecimal value2, BigDecimal value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(BigDecimal value) {
            return eq(value, value1) || eq(value, value2) || eq(value, value3);
        }
    }

    private final static class InverseThreeValueBigDecimalChunkFilter extends ObjectChunkFilter<BigDecimal> {
        private final BigDecimal value1;
        private final BigDecimal value2;
        private final BigDecimal value3;

        private InverseThreeValueBigDecimalChunkFilter(BigDecimal value1, BigDecimal value2, BigDecimal value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(BigDecimal value) {
            return !(eq(value, value1) || eq(value, value2) || eq(value, value3));
        }
    }

    /**
     * The values of a multi-value filter, sorted so that a binary search, which compares by {@code compareTo}, finds
     * them.
     */
    private static final class SortedValues {
        private final BigDecimal[] sorted;
        private final boolean containsNull;

        private SortedValues(final Object... values) {
            sorted = Arrays.stream(values).filter(value -> value != null).map(value -> (BigDecimal) value).sorted()
                    .toArray(BigDecimal[]::new);
            containsNull = sorted.length < values.length;
        }

        private boolean contains(final BigDecimal value) {
            return value == null ? containsNull : Arrays.binarySearch(sorted, value) >= 0;
        }
    }

    private final static class MultiValueBigDecimalChunkFilter extends ObjectChunkFilter<BigDecimal> {
        private final SortedValues values;

        private MultiValueBigDecimalChunkFilter(Object... values) {
            this.values = new SortedValues(values);
        }

        @Override
        public boolean matches(BigDecimal value) {
            return values.contains(value);
        }
    }

    private final static class InverseMultiValueBigDecimalChunkFilter extends ObjectChunkFilter<BigDecimal> {
        private final SortedValues values;

        private InverseMultiValueBigDecimalChunkFilter(Object... values) {
            this.values = new SortedValues(values);
        }

        @Override
        public boolean matches(BigDecimal value) {
            return !values.contains(value);
        }
    }
}
