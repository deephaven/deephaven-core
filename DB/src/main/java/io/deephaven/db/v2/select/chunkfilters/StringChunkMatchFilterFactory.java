package io.deephaven.db.v2.select.chunkfilters;

import io.deephaven.hash.KeyedObjectHashSet;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.base.string.cache.CharSequenceUtils;
import io.deephaven.db.v2.select.ChunkFilter;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;

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

    static ChunkFilter.ObjectChunkFilter makeCaseInsensitiveFilter(boolean invert, Object... values) {
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

    private static class SingleValueStringChunkFilter implements ChunkFilter.ObjectChunkFilter<String> {
        private final String value;

        private SingleValueStringChunkFilter(String value) {
            this.value = value;
        }

        @Override
        public void filter(ObjectChunk<String, ? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results) {
            final ObjectChunk<String, ? extends Values> stringChunk = values.asTypedObjectChunk();
            results.setSize(0);
            for (int ii = 0; ii < stringChunk.size(); ++ii) {
                final String checkString = stringChunk.get(ii);
                if (value.equalsIgnoreCase(checkString)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseSingleValueStringChunkFilter implements ChunkFilter.ObjectChunkFilter<String> {
        private final String value;

        private InverseSingleValueStringChunkFilter(String value) {
            this.value = value;
        }

        @Override
        public void filter(ObjectChunk<String, ? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results) {
            final ObjectChunk<String, ? extends Values> stringChunk = values.asTypedObjectChunk();
            results.setSize(0);
            for (int ii = 0; ii < stringChunk.size(); ++ii) {
                final String checkString = stringChunk.get(ii);
                if (!value.equalsIgnoreCase(checkString)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class TwoValueStringChunkFilter implements ChunkFilter.ObjectChunkFilter<String> {
        private final String value1;
        private final String value2;

        private TwoValueStringChunkFilter(String value1, String value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public void filter(ObjectChunk<String, ? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results) {
            final ObjectChunk<String, ? extends Values> stringChunk = values.asTypedObjectChunk();
            results.setSize(0);
            for (int ii = 0; ii < stringChunk.size(); ++ii) {
                final String checkString = stringChunk.get(ii);
                if (value1.equalsIgnoreCase(checkString) || value2.equalsIgnoreCase(checkString)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseTwoValueStringChunkFilter implements ChunkFilter.ObjectChunkFilter<String> {
        private final String value1;
        private final String value2;

        private InverseTwoValueStringChunkFilter(String value1, String value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public void filter(ObjectChunk<String, ? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results) {
            final ObjectChunk<String, ? extends Values> stringChunk = values.asTypedObjectChunk();
            results.setSize(0);
            for (int ii = 0; ii < stringChunk.size(); ++ii) {
                final String checkString = stringChunk.get(ii);
                if (!(value1.equalsIgnoreCase(checkString) || value2.equalsIgnoreCase(checkString))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class ThreeValueStringChunkFilter implements ChunkFilter.ObjectChunkFilter<String> {
        private final String value1;
        private final String value2;
        private final String value3;

        private ThreeValueStringChunkFilter(String value1, String value2, String value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public void filter(ObjectChunk<String, ? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results) {
            final ObjectChunk<String, ? extends Values> stringChunk = values.asTypedObjectChunk();
            results.setSize(0);
            for (int ii = 0; ii < stringChunk.size(); ++ii) {
                final String checkString = stringChunk.get(ii);
                if (value1.equalsIgnoreCase(checkString) || value2.equalsIgnoreCase(checkString)
                        || value3.equalsIgnoreCase(checkString)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseThreeValueStringChunkFilter implements ChunkFilter.ObjectChunkFilter<String> {
        private final String value1;
        private final String value2;
        private final String value3;

        private InverseThreeValueStringChunkFilter(String value1, String value2, String value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public void filter(ObjectChunk<String, ? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results) {
            final ObjectChunk<String, ? extends Values> stringChunk = values.asTypedObjectChunk();
            results.setSize(0);
            for (int ii = 0; ii < stringChunk.size(); ++ii) {
                final String checkString = stringChunk.get(ii);
                if (!(value1.equalsIgnoreCase(checkString) || value2.equalsIgnoreCase(checkString)
                        || value3.equalsIgnoreCase(checkString))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class MultiValueStringChunkFilter implements ChunkFilter.ObjectChunkFilter<String> {
        private final KeyedObjectHashSet<String, String> values;

        private MultiValueStringChunkFilter(Object... values) {
            this.values = new KeyedObjectHashSet<>(CASE_INSENSITIVE_KEY_INSTANCE);
            for (Object value : values) {
                this.values.add((String) value);
            }
        }

        @Override
        public void filter(ObjectChunk<String, ? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results) {
            final ObjectChunk<String, ? extends Values> stringChunk = values.asTypedObjectChunk();
            results.setSize(0);
            for (int ii = 0; ii < stringChunk.size(); ++ii) {
                final String checkString = stringChunk.get(ii);
                if (this.values.containsKey(checkString)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    private static class InverseMultiValueStringChunkFilter implements ChunkFilter.ObjectChunkFilter<String> {
        private final KeyedObjectHashSet<String, String> values;

        private InverseMultiValueStringChunkFilter(Object... values) {
            this.values = new KeyedObjectHashSet<>(CASE_INSENSITIVE_KEY_INSTANCE);
            for (Object value : values) {
                this.values.add((String) value);
            }
        }

        @Override
        public void filter(ObjectChunk<String, ? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results) {
            final ObjectChunk<String, ? extends Values> stringChunk = values.asTypedObjectChunk();
            results.setSize(0);
            for (int ii = 0; ii < stringChunk.size(); ++ii) {
                final String checkString = stringChunk.get(ii);
                if (!this.values.containsKey(checkString)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }
}
