//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.staticpercentile;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

/**
 * Percentile operator for static tables. Each destination accumulates its non-null values into an array; when the
 * initial state is propagated, an introselect finds the value at the percentile's position in each array. The arrays
 * are released once the results are computed.
 * <p>
 * The selected positions match {@link io.deephaven.engine.table.impl.by.ssmpercentile.SsmChunkedPercentileOperator}.
 * With {@code averageEvenlyDivided}, the low count is {@code (int) ((size - 1) * percentile) + 1}, and when it is
 * exactly half of the values the result averages the low maximum with the high minimum. Otherwise the low count is
 * {@code Math.round((size - 1) * percentile) + 1} and the result is the low maximum.
 */
public abstract class StaticPercentileOperator implements IterativeChunkedAggregationOperator {
    final double percentile;
    final String name;

    /**
     * The number of values accumulated for each destination; meaningful only once the destination has an array.
     */
    final IntegerArraySource sizes = new IntegerArraySource();

    StaticPercentileOperator(final double percentile, final String name) {
        this.percentile = percentile;
        this.name = name;
    }

    /**
     * Make a static percentile operator for {@code type}.
     *
     * @param type the data type of the values
     * @param equalsConsistent true when values of the type compare equal exactly when they are equal (see
     *        {@link io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper#compareConsistentWithEquality(Class)});
     *        when false, an Object result is the first value received among those that compare equal to it
     * @param percentile the percentile to compute
     * @param averageEvenlyDivided see {@link io.deephaven.api.agg.spec.AggSpecPercentile#averageEvenlyDivided()}
     * @param name the name of the result column
     * @return the operator
     */
    public static IterativeChunkedAggregationOperator make(final Class<?> type, final boolean equalsConsistent,
            final double percentile, final boolean averageEvenlyDivided, final String name) {
        if (type == char.class) {
            return new CharStaticPercentileOperator(percentile, averageEvenlyDivided, name);
        }
        if (type == byte.class) {
            return new ByteStaticPercentileOperator(percentile, averageEvenlyDivided, name);
        }
        if (type == short.class) {
            return new ShortStaticPercentileOperator(percentile, averageEvenlyDivided, name);
        }
        if (type == int.class) {
            return new IntStaticPercentileOperator(percentile, averageEvenlyDivided, name);
        }
        if (type == long.class) {
            return new LongStaticPercentileOperator(percentile, averageEvenlyDivided, name);
        }
        if (type == float.class) {
            return new FloatStaticPercentileOperator(percentile, averageEvenlyDivided, name);
        }
        if (type == double.class) {
            return new DoubleStaticPercentileOperator(percentile, averageEvenlyDivided, name);
        }
        if (type == Instant.class) {
            return new InstantStaticPercentileOperator(percentile, name);
        }
        if (type.isPrimitive()) {
            throw new UnsupportedOperationException("Percentile is not supported for " + type);
        }
        return new ObjectStaticPercentileOperator(type, equalsConsistent, percentile, name);
    }

    /**
     * @return the column source that holds the results
     */
    abstract WritableColumnSource<?> resultColumn();

    /**
     * Ensure that the per-destination state can hold destinations up to {@code capacity - 1}.
     */
    abstract void ensureArraysCapacity(long capacity);

    /**
     * @return the capacity for an array that holds {@code currentSize + length} values, given its current
     *         {@code capacity}
     */
    static int grownCapacity(final int capacity, final int currentSize, final int length) {
        final long required = (long) currentSize + length;
        return (int) Math.min(ArrayUtil.MAX_ARRAY_SIZE, Math.max(required, 2L * capacity));
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        throw Assert.statementNeverExecuted("removeChunk on a static percentile operator");
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        throw Assert.statementNeverExecuted("removeChunk on a static percentile operator");
    }

    @Override
    public void ensureCapacity(long tableSize) {
        resultColumn().ensureCapacity(tableSize);
        sizes.ensureCapacity(tableSize);
        ensureArraysCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.<String, ColumnSource<?>>singletonMap(name, resultColumn());
    }

    @Override
    public void startTrackingPrevValues() {
        throw Assert.statementNeverExecuted("startTrackingPrevValues on a static percentile operator");
    }
}
