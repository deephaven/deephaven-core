//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

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

    @SuppressWarnings("rawtypes")
    static ObjectChunkFilter makeCaseInsensitiveFilter(boolean invert, Object... values) {
        if (invert) {
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
            return this.value.equalsIgnoreCase(value);
        }
    }

    private static class InverseSingleValueStringChunkFilter extends ObjectChunkFilter<String> {
        private final String value;

        private InverseSingleValueStringChunkFilter(String value) {
            this.value = value;
        }

        @Override
        public boolean matches(String value) {
            return !this.value.equalsIgnoreCase(value);
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
            return value1.equalsIgnoreCase(value) || value2.equalsIgnoreCase(value);
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
            return !value1.equalsIgnoreCase(value) && !value2.equalsIgnoreCase(value);
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
            return value1.equalsIgnoreCase(value) || value2.equalsIgnoreCase(value) || value3.equalsIgnoreCase(value);
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
            return !value1.equalsIgnoreCase(value) && !value2.equalsIgnoreCase(value)
                    && !value3.equalsIgnoreCase(value);
        }
    }

    private static class MultiValueStringChunkFilter extends ObjectChunkFilter<String> {
        private final KeyedObjectHashSet<String, String> values;

        private MultiValueStringChunkFilter(Object... values) {
            this.values = new KeyedObjectHashSet<>(CASE_INSENSITIVE_KEY_INSTANCE);
            for (Object value : values) {
                this.values.add((String) value);
            }
        }

        @Override
        public boolean matches(String value) {
            return this.values.containsKey(value);
        }
    }

    private static class InverseMultiValueStringChunkFilter extends ObjectChunkFilter<String> {
        private final KeyedObjectHashSet<String, String> values;

        private InverseMultiValueStringChunkFilter(Object... values) {
            this.values = new KeyedObjectHashSet<>(CASE_INSENSITIVE_KEY_INSTANCE);
            for (Object value : values) {
                this.values.add((String) value);
            }
        }

        @Override
        public boolean matches(String value) {
            return !this.values.containsKey(value);
        }
    }
}
