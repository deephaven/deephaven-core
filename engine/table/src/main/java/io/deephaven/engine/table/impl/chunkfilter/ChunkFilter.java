//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;

public interface ChunkFilter {
    /**
     * Filter a chunk of values, setting parallel values in results to "true" or "false".
     * <p>
     * The results chunk must have capacity at least as large as values.size(); and the result size will be set to
     * values.size() on return.
     * 
     * @param values the values to filter
     * @param results a chunk with the key values where the result of the filter is true
     */
    void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
            WritableLongChunk<OrderedRowKeys> results);

    /**
     * Filter a chunk of values, setting parallel values in {@code results} to the output of the filter.
     * 
     * @param values the values to filter
     * @param results a boolean chunk containing the result of the filter
     *
     * @return the number of values that are {@code true} in {@code results} after the filter is applied.
     */
    int filter(Chunk<? extends Values> values, WritableBooleanChunk<Values> results);

    /**
     * Filter a chunk of values, setting parallel values in {@code results} to {@code false} when the filter result is
     * {@code false}. The filter will not be evaluated for values that are already {@code false} in the results chunk.
     * <p>
     * To use this method effectively, the results chunk should be initialized by a call to
     * {@link #filter(Chunk, WritableBooleanChunk)} or by setting all values {@code true} before the first call.
     * Successive calls will have the effect of AND'ing the filter results with existing results.
     *
     * @param values the values to filter
     * @param results a boolean chunk containing the result of the filter
     *
     * @return the number of values that are {@code true} in {@code results} after the filter is applied.
     */
    int filterAnd(Chunk<? extends Values> values, WritableBooleanChunk<Values> results);

    /**
     * Filter a chunk of values, setting parallel values in {@code results} to {@code true} when the filter result is
     * {@code true}. The filter will not be evaluated for values that are already {@code true} in the results chunk.
     * <p>
     * To use this method effectively, the results chunk should be initialized by a call to
     * {@link #filter(Chunk, WritableBooleanChunk)} or by setting all values {@code false} before the first call.
     * Successive calls will have the effect of {@code OR}'ing the filter results with existing results.`
     *
     * @param values the values to filter
     * @param results a boolean chunk containing the result of the filter
     *
     * @return the number of values that are {@code true} in {@code results} after the filter is applied.
     */
    int filterOr(Chunk<? extends Values> values, WritableBooleanChunk<Values> results);

    abstract class CharChunkFilter implements ChunkFilter {
        public abstract boolean matches(char value);

        @Override
        public final void filter(
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
        public final int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
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
        public final int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
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

        @Override
        public final int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    // already true, no need to compute, but must increment the count
                    count++;
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

    abstract class ByteChunkFilter implements ChunkFilter {
        public abstract boolean matches(byte value);

        @Override
        public final void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ByteChunk<? extends Values> byteChunk = values.asByteChunk();
            final int len = byteChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(byteChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public final int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final ByteChunk<? extends Values> byteChunk = values.asByteChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(byteChunk.get(ii));
                results.set(ii, newResult);
                // count every true value
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public final int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final ByteChunk<? extends Values> byteChunk = values.asByteChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    // already false, no need to compute or increment the count
                    continue;
                }
                boolean newResult = matches(byteChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public final int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final ByteChunk<? extends Values> byteChunk = values.asByteChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    // already true, no need to compute, but must increment the count
                    count++;
                    continue;
                }
                boolean newResult = matches(byteChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    abstract class ShortChunkFilter implements ChunkFilter {
        public abstract boolean matches(short value);

        @Override
        public final void filter(
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
        public final int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final ShortChunk<? extends Values> shortChunk = values.asShortChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(shortChunk.get(ii));
                results.set(ii, newResult);
                // count every true value
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public final int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final ShortChunk<? extends Values> shortChunk = values.asShortChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    // already false, no need to compute or increment the count
                    continue;
                }
                boolean newResult = matches(shortChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public final int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final ShortChunk<? extends Values> shortChunk = values.asShortChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    // already true, no need to compute, but must increment the count
                    count++;
                    continue;
                }
                boolean newResult = matches(shortChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    abstract class IntChunkFilter implements ChunkFilter {
        public abstract boolean matches(int value);

        @Override
        public final void filter(
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
        public final int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
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
        public final int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
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

        @Override
        public final int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    // already true, no need to compute, but must increment the count
                    count++;
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

    abstract class LongChunkFilter implements ChunkFilter {
        public abstract boolean matches(long value);

        @Override
        public final void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final LongChunk<? extends Values> longChunk = values.asLongChunk();
            final int len = longChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(longChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public final int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final LongChunk<? extends Values> longChunk = values.asLongChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(longChunk.get(ii));
                results.set(ii, newResult);
                // count every true value
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public final int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final LongChunk<? extends Values> longChunk = values.asLongChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    // already false, no need to compute or increment the count
                    continue;
                }
                boolean newResult = matches(longChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public final int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final LongChunk<? extends Values> longChunk = values.asLongChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    // already true, no need to compute, but must increment the count
                    count++;
                    continue;
                }
                boolean newResult = matches(longChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    abstract class FloatChunkFilter implements ChunkFilter {
        public abstract boolean matches(float value);

        @Override
        public final void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final FloatChunk<? extends Values> floatChunk = values.asFloatChunk();
            final int len = floatChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(floatChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public final int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final FloatChunk<? extends Values> floatChunk = values.asFloatChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(floatChunk.get(ii));
                results.set(ii, newResult);
                // count every true value
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public final int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final FloatChunk<? extends Values> floatChunk = values.asFloatChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    // already false, no need to compute or increment the count
                    continue;
                }
                boolean newResult = matches(floatChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public final int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final FloatChunk<? extends Values> floatChunk = values.asFloatChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    // already true, no need to compute, but must increment the count
                    count++;
                    continue;
                }
                boolean newResult = matches(floatChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    abstract class DoubleChunkFilter implements ChunkFilter {
        public abstract boolean matches(double value);

        @Override
        public final void filter(
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
        public final int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final DoubleChunk<? extends Values> doubleChunk = values.asDoubleChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(doubleChunk.get(ii));
                results.set(ii, newResult);
                // count every true value
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public final int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final DoubleChunk<? extends Values> doubleChunk = values.asDoubleChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    // already false, no need to compute or increment the count
                    continue;
                }
                boolean newResult = matches(doubleChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public final int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final DoubleChunk<? extends Values> doubleChunk = values.asDoubleChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    // already true, no need to compute, but must increment the count
                    count++;
                    continue;
                }
                boolean newResult = matches(doubleChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    abstract class ObjectChunkFilter<T> implements ChunkFilter {
        public abstract boolean matches(T value);

        @Override
        public final void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ObjectChunk<T, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = objectChunk.size();

            results.setSize(0);
            for (int ii = 0; ii < len; ++ii) {
                if (matches(objectChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }

        @Override
        public final int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final ObjectChunk<T, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean newResult = matches(objectChunk.get(ii));
                results.set(ii, newResult);
                // count every true value
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public final int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final ObjectChunk<T, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from true to false
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (!result) {
                    // already false, no need to compute or increment the count
                    continue;
                }
                boolean newResult = matches(objectChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }

        @Override
        public final int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final ObjectChunk<T, ? extends Values> objectChunk = values.asObjectChunk();
            final int len = values.size();
            int count = 0;
            // Count the values that changed from false to true
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                if (result) {
                    // already true, no need to compute, but must increment the count
                    count++;
                    continue;
                }
                boolean newResult = matches(objectChunk.get(ii));
                results.set(ii, newResult);
                // increment the count if the new result is TRUE
                count += newResult ? 1 : 0;
            }
            return count;
        }
    }

    /**
     * A filter that always returns false.
     */
    ChunkFilter FALSE_FILTER_INSTANCE = new ChunkFilter() {
        @Override
        public void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final int len = values.size();
            results.fillWithValue(0, len, false);
            return 0;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final int len = values.size();
            // Count the values that changed from true to false
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                results.set(ii, false);
                count += result ? 1 : 0;
            }
            return count;
        }

        @Override
        public int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            // No values were set to true
            return 0;
        }
    };

    /**
     * A filter that always returns true.
     */
    ChunkFilter TRUE_FILTER_INSTANCE = new ChunkFilter() {
        @Override
        public void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(values.size());
            results.copyFromChunk(keys, 0, 0, keys.size());
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final int len = values.size();
            results.fillWithValue(0, len, true);
            return len;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            // No values were set to false
            return 0;
        }

        @Override
        public int filterOr(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final int len = values.size();
            // Count the values that changed from false to true
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                final boolean result = results.get(ii);
                results.set(ii, true);
                count += result ? 0 : 1;
            }
            return count;
        }
    };

    /**
     * How many values we read from a column source into a chunk before applying a filter.
     */
    int FILTER_CHUNK_SIZE = 2048;
    /**
     * How many values we wait for before checking for interruption and throwing a cancellation exception
     */
    long INITIAL_INTERRUPTION_SIZE =
            Configuration.getInstance().getLongWithDefault("ChunkFilter.initialInterruptionSize", 1 << 20);
    /**
     * How long we would like to take, in milliseconds between interruption checks
     */
    long INTERRUPTION_GOAL_MILLIS =
            Configuration.getInstance().getLongWithDefault("ChunkFilter.interruptionGoalMillis", 100);

    /**
     * Apply a chunk filter to a RowSet and column source, producing a new WritableRowSet that is responsive to the
     * filter.
     *
     * @param selection the RowSet to filter
     * @param columnSource the column source to filter
     * @param usePrev should we use previous values from the column source?
     * @param chunkFilter the chunk filter to apply
     *
     * @return A new WritableRowSet representing the filtered values, owned by the caller
     */
    static WritableRowSet applyChunkFilter(RowSet selection, ColumnSource<?> columnSource, boolean usePrev,
            ChunkFilter chunkFilter) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();

        final int contextSize = (int) Math.min(FILTER_CHUNK_SIZE, selection.size());
        long chunksBetweenChecks = INITIAL_INTERRUPTION_SIZE / FILTER_CHUNK_SIZE;
        long filteredChunks = 0;
        long lastInterruptCheck = System.currentTimeMillis();

        try (final ColumnSource.GetContext getContext = columnSource.makeGetContext(contextSize);
                final WritableLongChunk<OrderedRowKeys> longChunk = WritableLongChunk.makeWritableChunk(contextSize);
                final RowSequence.Iterator rsIt = selection.getRowSequenceIterator()) {
            while (rsIt.hasMore()) {
                if (filteredChunks++ == chunksBetweenChecks) {
                    if (Thread.interrupted()) {
                        throw new CancellationException("interrupted while filtering data");
                    }

                    final long now = System.currentTimeMillis();
                    final long checkDuration = now - lastInterruptCheck;

                    // tune so that we check at the desired interval, never less than one chunk
                    chunksBetweenChecks = Math.max(1, Math.min(1, checkDuration <= 0 ? chunksBetweenChecks * 2
                            : chunksBetweenChecks * INTERRUPTION_GOAL_MILLIS / checkDuration));
                    lastInterruptCheck = now;
                    filteredChunks = 0;
                }
                final RowSequence okChunk = rsIt.getNextRowSequenceWithLength(contextSize);
                final LongChunk<OrderedRowKeys> keyChunk = okChunk.asRowKeyChunk();

                final Chunk<? extends Values> dataChunk;
                if (usePrev) {
                    dataChunk = columnSource.getPrevChunk(getContext, okChunk);
                } else {
                    dataChunk = columnSource.getChunk(getContext, okChunk);
                }
                chunkFilter.filter(dataChunk, keyChunk, longChunk);

                builder.appendOrderedRowKeysChunk(longChunk);
            }
        }
        return builder.build();
    }
}
