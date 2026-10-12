//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRangeComparator and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.util.compare.IntComparisons;

/**
 * Creates range filters for int values.
 * <p>
 * Each filter carries its own copy of the {@link IntChunkFilter} loops, so that its {@code matches} call is never a
 * virtual call shared with other filters.
 */
public class IntRangeComparator {
    private IntRangeComparator() {} // static use only

    private abstract static class IntIntFilter extends IntChunkFilter {
        final int lower;
        final int upper;

        IntIntFilter(int lower, int upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    private final static class IntIntInclusiveInclusiveFilter extends IntIntFilter {
        private IntIntInclusiveInclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(int value) {
            return IntComparisons.geq(value, lower) && IntComparisons.leq(value, upper);
        }

        // Identical code for all IntChunkFilter classes, replicated here to prevent megamorphism in the JVM
        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = intChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(intChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(intChunk.get(ii));
                results.set(ii, newResult);
                // count every true value
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that remain true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    // already false, no need to compute or increment the count
                    continue;
                }
                boolean newResult = matches(intChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    private final static class IntIntInclusiveExclusiveFilter extends IntIntFilter {
        private IntIntInclusiveExclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(int value) {
            return IntComparisons.geq(value, lower) && IntComparisons.lt(value, upper);
        }

        // Identical code for all IntChunkFilter classes, replicated here to prevent megamorphism in the JVM
        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = intChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(intChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(intChunk.get(ii));
                results.set(ii, newResult);
                // count every true value
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that remain true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    // already false, no need to compute or increment the count
                    continue;
                }
                boolean newResult = matches(intChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    private final static class IntIntExclusiveInclusiveFilter extends IntIntFilter {
        private IntIntExclusiveInclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(int value) {
            return IntComparisons.gt(value, lower) && IntComparisons.leq(value, upper);
        }

        // Identical code for all IntChunkFilter classes, replicated here to prevent megamorphism in the JVM
        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = intChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(intChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(intChunk.get(ii));
                results.set(ii, newResult);
                // count every true value
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that remain true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    // already false, no need to compute or increment the count
                    continue;
                }
                boolean newResult = matches(intChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    private final static class IntIntExclusiveExclusiveFilter extends IntIntFilter {
        private IntIntExclusiveExclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(int value) {
            return IntComparisons.gt(value, lower) && IntComparisons.lt(value, upper);
        }

        // Identical code for all IntChunkFilter classes, replicated here to prevent megamorphism in the JVM
        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = intChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(intChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(intChunk.get(ii));
                results.set(ii, newResult);
                // count every true value
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that remain true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    // already false, no need to compute or increment the count
                    continue;
                }
                boolean newResult = matches(intChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    public static IntChunkFilter makeIntFilter(int lower, int upper, boolean lowerInclusive,
            boolean upperInclusive) {
        if (lowerInclusive) {
            if (upperInclusive) {
                return new IntIntInclusiveInclusiveFilter(lower, upper);
            } else {
                return new IntIntInclusiveExclusiveFilter(lower, upper);
            }
        } else {
            if (upperInclusive) {
                return new IntIntExclusiveInclusiveFilter(lower, upper);
            } else {
                return new IntIntExclusiveExclusiveFilter(lower, upper);
            }
        }
    }
}
