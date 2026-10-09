//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.util.compare.CharComparisons;

/**
 * Creates range filters for char values.
 * <p>
 * Each filter carries its own copy of the {@link CharChunkFilter} loops, so that its {@code matches} call is never a
 * virtual call shared with other filters.
 */
public class CharRangeComparator {
    private CharRangeComparator() {} // static use only

    private abstract static class CharCharFilter extends CharChunkFilter {
        final char lower;
        final char upper;

        CharCharFilter(char lower, char upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    private final static class CharCharInclusiveInclusiveFilter extends CharCharFilter {
        private CharCharInclusiveInclusiveFilter(char lower, char upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(char value) {
            return CharComparisons.geq(value, lower) && CharComparisons.leq(value, upper);
        }

        // Identical code for all CharChunkFilter classes, replicated here to prevent megamorphism in the JVM
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
    }

    private final static class CharCharInclusiveExclusiveFilter extends CharCharFilter {
        private CharCharInclusiveExclusiveFilter(char lower, char upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(char value) {
            return CharComparisons.geq(value, lower) && CharComparisons.lt(value, upper);
        }

        // Identical code for all CharChunkFilter classes, replicated here to prevent megamorphism in the JVM
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
    }

    private final static class CharCharExclusiveInclusiveFilter extends CharCharFilter {
        private CharCharExclusiveInclusiveFilter(char lower, char upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(char value) {
            return CharComparisons.gt(value, lower) && CharComparisons.leq(value, upper);
        }

        // Identical code for all CharChunkFilter classes, replicated here to prevent megamorphism in the JVM
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
    }

    private final static class CharCharExclusiveExclusiveFilter extends CharCharFilter {
        private CharCharExclusiveExclusiveFilter(char lower, char upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(char value) {
            return CharComparisons.gt(value, lower) && CharComparisons.lt(value, upper);
        }

        // Identical code for all CharChunkFilter classes, replicated here to prevent megamorphism in the JVM
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
    }

    public static CharChunkFilter makeCharFilter(char lower, char upper, boolean lowerInclusive,
            boolean upperInclusive) {
        if (lowerInclusive) {
            if (upperInclusive) {
                return new CharCharInclusiveInclusiveFilter(lower, upper);
            } else {
                return new CharCharInclusiveExclusiveFilter(lower, upper);
            }
        } else {
            if (upperInclusive) {
                return new CharCharExclusiveInclusiveFilter(lower, upper);
            } else {
                return new CharCharExclusiveExclusiveFilter(lower, upper);
            }
        }
    }
}
