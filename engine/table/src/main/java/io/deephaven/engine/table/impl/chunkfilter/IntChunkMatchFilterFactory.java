//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkMatchFilterFactory and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.MatchOptions;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

/**
 * Creates chunk filters for int values.
 * <p>
 * The strategy is that for one, two, or three values we have specialized classes that will do the appropriate simple
 * equality check.
 * <p>
 * For more values, we use a trove set and check contains for each value in the chunk.
 * <p>
 * Each filter carries its own copy of the {@link IntChunkFilter} loops, so that its {@code matches} call is never a
 * virtual call shared with other filters.
 */
public class IntChunkMatchFilterFactory {
    private IntChunkMatchFilterFactory() {} // static use only

    public static IntChunkFilter makeFilter(final MatchOptions matchOptions, final int... values) {
        if (matchOptions.inverted()) {
            if (values.length == 1) {
                return new InverseSingleValueIntChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new InverseTwoValueIntChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new InverseThreeValueIntChunkFilter(values[0], values[1], values[2]);
            }
            return new InverseMultiValueIntChunkFilter(values);
        } else {
            if (values.length == 1) {
                return new SingleValueIntChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new TwoValueIntChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new ThreeValueIntChunkFilter(values[0], values[1], values[2]);
            }
            return new MultiValueIntChunkFilter(values);
        }
    }

    private final static class SingleValueIntChunkFilter extends IntChunkFilter {
        private final int value;

        private SingleValueIntChunkFilter(int value) {
            this.value = value;
        }

        @Override
        public boolean matches(int value) {
            return value == this.value;
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

    private final static class InverseSingleValueIntChunkFilter extends IntChunkFilter {
        private final int value;

        private InverseSingleValueIntChunkFilter(int value) {
            this.value = value;
        }

        @Override
        public boolean matches(int value) {
            return value != this.value;
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

    private final static class TwoValueIntChunkFilter extends IntChunkFilter {
        private final int value1;
        private final int value2;

        private TwoValueIntChunkFilter(int value1, int value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(int value) {
            return value == value1 || value == value2;
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

    private final static class InverseTwoValueIntChunkFilter extends IntChunkFilter {
        private final int value1;
        private final int value2;

        private InverseTwoValueIntChunkFilter(int value1, int value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(int value) {
            return value != value1 && value != value2;
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

    private final static class ThreeValueIntChunkFilter extends IntChunkFilter {
        private final int value1;
        private final int value2;
        private final int value3;

        private ThreeValueIntChunkFilter(int value1, int value2, int value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(int value) {
            return value == value1 || value == value2 || value == value3;
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

    private final static class InverseThreeValueIntChunkFilter extends IntChunkFilter {
        private final int value1;
        private final int value2;
        private final int value3;

        private InverseThreeValueIntChunkFilter(int value1, int value2, int value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(int value) {
            return value != value1 && value != value2 && value != value3;
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

    private final static class MultiValueIntChunkFilter extends IntChunkFilter {
        private final IntSet values;

        private MultiValueIntChunkFilter(int... values) {
            this.values = new IntOpenHashSet(values);
        }

        @Override
        public boolean matches(int value) {
            return this.values.contains(value);
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

    private final static class InverseMultiValueIntChunkFilter extends IntChunkFilter {
        private final IntSet values;

        private InverseMultiValueIntChunkFilter(int... values) {
            this.values = new IntOpenHashSet(values);
        }

        @Override
        public boolean matches(int value) {
            return !this.values.contains(value);
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
}
