//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRangeComparator and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.util.compare.ShortComparisons;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;

public class ShortRangeComparator {
    private ShortRangeComparator() {} // static use only

    private abstract static class ShortShortFilter implements ChunkFilter.ShortChunkFilter {
        final short lower;
        final short upper;

        ShortShortFilter(short lower, short upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    static class ShortShortInclusiveInclusiveFilter extends ShortShortFilter {
        private ShortShortInclusiveInclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(short value) {
            return ShortComparisons.geq(value, lower) && ShortComparisons.leq(value, upper);
        }

        /*
         * The following functions are identical and repeated for each of the filter types. This is to aid the JVM in
         * correctly inlining the matches() function. The goal is to have a single virtual call per chunk rather than
         * once per value. This improves performance on JVM <= 21, but may be unnecessary on newer JVMs.
         */
        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ShortChunk<? extends Values> shortChunk = values.asShortChunk();
            final int len = shortChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(shortChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final ShortChunk<? extends Values> shortChunk = values.asShortChunk();
            final int len = shortChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(shortChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    static class ShortShortInclusiveExclusiveFilter extends ShortShortFilter {
        private ShortShortInclusiveExclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(short value) {
            return ShortComparisons.geq(value, lower) && ShortComparisons.lt(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ShortChunk<? extends Values> shortChunk = values.asShortChunk();
            final int len = shortChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(shortChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final ShortChunk<? extends Values> shortChunk = values.asShortChunk();
            final int len = shortChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(shortChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    static class ShortShortExclusiveInclusiveFilter extends ShortShortFilter {
        private ShortShortExclusiveInclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(short value) {
            return ShortComparisons.gt(value, lower) && ShortComparisons.leq(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ShortChunk<? extends Values> shortChunk = values.asShortChunk();
            final int len = shortChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(shortChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final ShortChunk<? extends Values> shortChunk = values.asShortChunk();
            final int len = shortChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(shortChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    static class ShortShortExclusiveExclusiveFilter extends ShortShortFilter {
        private ShortShortExclusiveExclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(short value) {
            return ShortComparisons.gt(value, lower) && ShortComparisons.lt(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ShortChunk<? extends Values> shortChunk = values.asShortChunk();
            final int len = shortChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(shortChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            final ShortChunk<? extends Values> shortChunk = values.asShortChunk();
            final int len = shortChunk.size();

            int count = 0;
            // ideally branchless implementation
            for (int ii = 0; ii < len; ++ii) {
                boolean result = results.get(ii);
                boolean newResult = result & matches(shortChunk.get(ii));
                results.set(ii, newResult);
                count += result == newResult ? 0 : 1;
            }
            return count;
        }
    }

    public static ChunkFilter.ShortChunkFilter makeShortFilter(short lower, short upper, boolean lowerInclusive,
            boolean upperInclusive) {
        if (lowerInclusive) {
            if (upperInclusive) {
                return new ShortShortInclusiveInclusiveFilter(lower, upper);
            } else {
                return new ShortShortInclusiveExclusiveFilter(lower, upper);
            }
        } else {
            if (upperInclusive) {
                return new ShortShortExclusiveInclusiveFilter(lower, upper);
            } else {
                return new ShortShortExclusiveExclusiveFilter(lower, upper);
            }
        }
    }
}
