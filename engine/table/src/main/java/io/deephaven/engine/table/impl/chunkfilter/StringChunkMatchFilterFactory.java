//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.hash.KeyedObjectHashSet;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.base.string.cache.CharSequenceUtils;

class StringChunkMatchFilterFactory {
    private static final class CIStringKey implements KeyedObjectKey<String, String> {
        @Override
        public String getKey(String s) {
            return s;
        }

        @Override
        public int hashKey(String s) {
            return (s == null) ? 0 : CharSequenceUtils.caseInsensitiveHashCode(s);
        }

        @Override
        public boolean equalKey(String s, String s2) {
            return (s == null) ? s2 == null : s.equalsIgnoreCase(s2);
        }
    }

    private final static CIStringKey CASE_INSENSITIVE_KEY_INSTANCE = new CIStringKey();

    private StringChunkMatchFilterFactory() {} // static use only

    /**
     * Case-insensitive equality where either side may be null.
     *
     * <p>
     * The receiver is the <em>filter's</em> value, so {@code filterValue.equalsIgnoreCase(columnValue)} throws when the
     * filter is given a null to match -- for every row, whatever the column holds. Null matches only null, which is
     * both what {@link CIStringKey#equalKey} does and what case-sensitive {@code in} does; case folding cannot
     * distinguish null from anything else, so {@code icase} must agree with {@code in} on that input.
     */
    private static boolean equalsIgnoreCaseNullSafe(final String filterValue, final String columnValue) {
        return filterValue == null ? columnValue == null : filterValue.equalsIgnoreCase(columnValue);
    }

    /**
     * A case-insensitive set of match values, for the arities with too many values to specialize.
     *
     * <p>
     * {@link KeyedObjectHashSet} cannot hold a null, and adding one is dropped rather than refused, so null membership
     * is tracked separately instead of being silently lost.
     */
    private final static class CaseInsensitiveValueSet {
        private final KeyedObjectHashSet<String, String> values =
                new KeyedObjectHashSet<>(CASE_INSENSITIVE_KEY_INSTANCE);
        private final boolean matchesNull;

        private CaseInsensitiveValueSet(final Object... values) {
            boolean matchesNull = false;
            for (final Object value : values) {
                if (value == null) {
                    matchesNull = true;
                } else {
                    this.values.add((String) value);
                }
            }
            this.matchesNull = matchesNull;
        }

        private boolean contains(final String columnValue) {
            return columnValue == null ? matchesNull : values.containsKey(columnValue);
        }
    }

    /**
     * Create a case-insensitive filter for the provided values. Assumes that matchOptions.caseInsensitive() is true and
     * all provided values are {@link String}.
     */
    @SuppressWarnings("rawtypes")
    static ObjectChunkFilter makeCaseInsensitiveFilter(final MatchOptions matchOptions, final Object... values) {
        Assert.eqTrue(matchOptions.caseInsensitive(), "matchOptions.caseInsensitive()");
        if (matchOptions.inverted()) {
            if (values.length == 1) {
                return new InverseSingleValueStringChunkFilter((String) values[0]);
            }
            if (values.length == 2) {
                return new InverseTwoValueStringChunkFilter((String) values[0], (String) values[1]);
            }
            if (values.length == 3) {
                return new InverseThreeValueStringChunkFilter((String) values[0], (String) values[1],
                        (String) values[2]);
            }
            return new InverseMultiValueStringChunkFilter(values);
        } else {
            if (values.length == 1) {
                return new SingleValueStringChunkFilter((String) values[0]);
            }
            if (values.length == 2) {
                return new TwoValueStringChunkFilter((String) values[0], (String) values[1]);
            }
            if (values.length == 3) {
                return new ThreeValueStringChunkFilter((String) values[0], (String) values[1], (String) values[2]);
            }
            return new MultiValueStringChunkFilter(values);
        }
    }

    private final static class SingleValueStringChunkFilter extends ObjectChunkFilter<String> {
        private final String value;

        private SingleValueStringChunkFilter(String value) {
            this.value = value;
        }

        @Override
        public boolean matches(String value) {
            return equalsIgnoreCaseNullSafe(this.value, value);
        }
    }

    private static class InverseSingleValueStringChunkFilter extends ObjectChunkFilter<String> {
        private final String value;

        private InverseSingleValueStringChunkFilter(String value) {
            this.value = value;
        }

        @Override
        public boolean matches(String value) {
            return !equalsIgnoreCaseNullSafe(this.value, value);
        }
    }

    private static class TwoValueStringChunkFilter extends ObjectChunkFilter<String> {
        private final String value1;
        private final String value2;

        private TwoValueStringChunkFilter(String value1, String value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(String value) {
            return equalsIgnoreCaseNullSafe(value1, value) || equalsIgnoreCaseNullSafe(value2, value);
        }
    }

    private static class InverseTwoValueStringChunkFilter extends ObjectChunkFilter<String> {
        private final String value1;
        private final String value2;

        private InverseTwoValueStringChunkFilter(String value1, String value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(String value) {
            return !equalsIgnoreCaseNullSafe(value1, value) && !equalsIgnoreCaseNullSafe(value2, value);
        }
    }

    private static class ThreeValueStringChunkFilter extends ObjectChunkFilter<String> {
        private final String value1;
        private final String value2;
        private final String value3;

        private ThreeValueStringChunkFilter(String value1, String value2, String value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(String value) {
            return equalsIgnoreCaseNullSafe(value1, value) || equalsIgnoreCaseNullSafe(value2, value)
                    || equalsIgnoreCaseNullSafe(value3, value);
        }
    }

    private static class InverseThreeValueStringChunkFilter extends ObjectChunkFilter<String> {
        private final String value1;
        private final String value2;
        private final String value3;

        private InverseThreeValueStringChunkFilter(String value1, String value2, String value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(String value) {
            return !equalsIgnoreCaseNullSafe(value1, value) && !equalsIgnoreCaseNullSafe(value2, value)
                    && !equalsIgnoreCaseNullSafe(value3, value);
        }
    }

    private static class MultiValueStringChunkFilter extends ObjectChunkFilter<String> {
        private final CaseInsensitiveValueSet values;

        private MultiValueStringChunkFilter(Object... values) {
            this.values = new CaseInsensitiveValueSet(values);
        }

        @Override
        public boolean matches(String value) {
            return this.values.contains(value);
        }
    }

    private static class InverseMultiValueStringChunkFilter extends ObjectChunkFilter<String> {
        private final CaseInsensitiveValueSet values;

        private InverseMultiValueStringChunkFilter(Object... values) {
            this.values = new CaseInsensitiveValueSet(values);
        }

        @Override
        public boolean matches(String value) {
            return !this.values.contains(value);
        }
    }
}
