//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatRangeComparator and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;

public class DoubleRangeComparator {
    private DoubleRangeComparator() {} // static use only

    private abstract static class DoubleDoubleFilter implements ChunkFilter.DoubleChunkFilter {
        final double lower;
        final double upper;

        DoubleDoubleFilter(double lower, double upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    static class DoubleDoubleInclusiveInclusiveFilter extends DoubleDoubleFilter {
        private DoubleDoubleInclusiveInclusiveFilter(double lower, double upper) {
            super(lower, upper);
        }

        private boolean matches(double value) {
            return DoubleComparisons.geq(value, lower) && DoubleComparisons.leq(value, upper);
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
            final DoubleChunk<? extends Values> doubleChunk = values.asDoubleChunk();
            final int len = doubleChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(doubleChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final DoubleChunk<? extends Values> typedChunk = values.asDoubleChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final DoubleChunk<? extends Values> typedChunk = values.asDoubleChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    continue; // already false, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 0 : 1;
            }
            return count;
        }

        @Override
        public int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final DoubleChunk<? extends Values> typedChunk = values.asDoubleChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    continue; // already true, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    static class DoubleDoubleInclusiveExclusiveFilter extends DoubleDoubleFilter {
        private DoubleDoubleInclusiveExclusiveFilter(double lower, double upper) {
            super(lower, upper);
        }

        private boolean matches(double value) {
            return DoubleComparisons.geq(value, lower) && DoubleComparisons.lt(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final DoubleChunk<? extends Values> doubleChunk = values.asDoubleChunk();
            final int len = doubleChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(doubleChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final DoubleChunk<? extends Values> typedChunk = values.asDoubleChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final DoubleChunk<? extends Values> typedChunk = values.asDoubleChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    continue; // already false, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 0 : 1;
            }
            return count;
        }

        @Override
        public int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final DoubleChunk<? extends Values> typedChunk = values.asDoubleChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    continue; // already true, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    static class DoubleDoubleExclusiveInclusiveFilter extends DoubleDoubleFilter {
        private DoubleDoubleExclusiveInclusiveFilter(double lower, double upper) {
            super(lower, upper);
        }

        private boolean matches(double value) {
            return DoubleComparisons.gt(value, lower) && DoubleComparisons.leq(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final DoubleChunk<? extends Values> doubleChunk = values.asDoubleChunk();
            final int len = doubleChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(doubleChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final DoubleChunk<? extends Values> typedChunk = values.asDoubleChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final DoubleChunk<? extends Values> typedChunk = values.asDoubleChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    continue; // already false, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 0 : 1;
            }
            return count;
        }

        @Override
        public int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final DoubleChunk<? extends Values> typedChunk = values.asDoubleChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    continue; // already true, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    static class DoubleDoubleExclusiveExclusiveFilter extends DoubleDoubleFilter {
        private DoubleDoubleExclusiveExclusiveFilter(double lower, double upper) {
            super(lower, upper);
        }

        private boolean matches(double value) {
            return DoubleComparisons.gt(value, lower) && DoubleComparisons.lt(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final DoubleChunk<? extends Values> doubleChunk = values.asDoubleChunk();
            final int len = doubleChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(doubleChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final DoubleChunk<? extends Values> typedChunk = values.asDoubleChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final DoubleChunk<? extends Values> typedChunk = values.asDoubleChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    continue; // already false, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 0 : 1;
            }
            return count;
        }

        @Override
        public int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final DoubleChunk<? extends Values> typedChunk = values.asDoubleChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    continue; // already true, no need to compute
                }
                boolean newResult = matches(typedChunk.get(ii));
                results.set(ii, newResult);
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    public static ChunkFilter.DoubleChunkFilter makeDoubleFilter(double lower, double upper, boolean lowerInclusive,
            boolean upperInclusive) {
        if (lowerInclusive) {
            if (upperInclusive) {
                return new DoubleDoubleInclusiveInclusiveFilter(lower, upper);
            } else {
                return new DoubleDoubleInclusiveExclusiveFilter(lower, upper);
            }
        } else {
            if (upperInclusive) {
                return new DoubleDoubleExclusiveInclusiveFilter(lower, upper);
            } else {
                return new DoubleDoubleExclusiveExclusiveFilter(lower, upper);
            }
        }
    }
}
