//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.MatchOptions;
import it.unimi.dsi.fastutil.chars.CharOpenHashSet;
import it.unimi.dsi.fastutil.chars.CharSet;

/**
 * Creates chunk filters for char values.
 * <p>
 * The strategy is that for one, two, or three values we have specialized classes that will do the appropriate simple
 * equality check.
 * <p>
 * For more values, we use a trove set and check contains for each value in the chunk.
 * <p>
 * The one, two, and three value filters each carry their own copy of the {@link CharChunkFilter} loops (the
 * {@code filterLoops} regions, filled in by {@code ReplicateChunkFilters}), so that their cheap {@code matches} call is
 * never a virtual call shared with other filters. The set-based filters use the shared loops, where the set lookup
 * outweighs the call.
 */
public class CharChunkMatchFilterFactory {
    private CharChunkMatchFilterFactory() {} // static use only

    public static CharChunkFilter makeFilter(final MatchOptions matchOptions, final char... values) {
        if (matchOptions.inverted()) {
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

        // region filterLoops
        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(charChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(charChunk.get(ii));
                results.set(ii, newResult);
                // count every true value
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that remain true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    // already false, no need to compute or increment the count
                    continue;
                }
                boolean newResult = matches(charChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }
        // endregion filterLoops
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

        // region filterLoops
        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(charChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(charChunk.get(ii));
                results.set(ii, newResult);
                // count every true value
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that remain true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    // already false, no need to compute or increment the count
                    continue;
                }
                boolean newResult = matches(charChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }
        // endregion filterLoops
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

        // region filterLoops
        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(charChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(charChunk.get(ii));
                results.set(ii, newResult);
                // count every true value
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that remain true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    // already false, no need to compute or increment the count
                    continue;
                }
                boolean newResult = matches(charChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }
        // endregion filterLoops
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

        // region filterLoops
        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(charChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(charChunk.get(ii));
                results.set(ii, newResult);
                // count every true value
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that remain true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    // already false, no need to compute or increment the count
                    continue;
                }
                boolean newResult = matches(charChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }
        // endregion filterLoops
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

        // region filterLoops
        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(charChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(charChunk.get(ii));
                results.set(ii, newResult);
                // count every true value
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that remain true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    // already false, no need to compute or increment the count
                    continue;
                }
                boolean newResult = matches(charChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }
        // endregion filterLoops
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

        // region filterLoops
        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = charChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(charChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(charChunk.get(ii));
                results.set(ii, newResult);
                // count every true value
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that remain true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    // already false, no need to compute or increment the count
                    continue;
                }
                boolean newResult = matches(charChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }
        // endregion filterLoops
    }

    private final static class MultiValueCharChunkFilter extends CharChunkFilter {
        private final CharSet values;

        private MultiValueCharChunkFilter(char... values) {
            this.values = new CharOpenHashSet(values);
        }

        @Override
        public boolean matches(char value) {
            return this.values.contains(value);
        }
    }

    private final static class InverseMultiValueCharChunkFilter extends CharChunkFilter {
        private final CharSet values;

        private InverseMultiValueCharChunkFilter(char... values) {
            this.values = new CharOpenHashSet(values);
        }

        @Override
        public boolean matches(char value) {
            return !this.values.contains(value);
        }
    }
}
