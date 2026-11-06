//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit BaseCharChunkedUniqueOperator and run "./gradlew replicateSegmentedSortedMultiset" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.ssmcountdistinct.unique;

import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.BucketSsmDistinctContext;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.SsmDistinctContext;
import io.deephaven.engine.table.impl.sources.ByteArraySource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.util.compact.ByteCompactKernel;
import io.deephaven.util.compare.ByteComparisons;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Map;

/**
 * Computes the single unique value of each aggregated state. If there are no values at all the 'no value key' is used.
 * If there are more than one values for the state, the 'non unique key' is used.
 *
 * <p>
 * Each chunk of added values is sorted and compacted to distinct values with counts, then applied to a state by
 * {@link #addToState}; subclasses define how a state is represented.
 */
abstract class BaseByteChunkedUniqueOperator implements IterativeChunkedAggregationOperator {
    protected final String name;

    protected final boolean countNullNaN;

    protected final ByteArraySource internalResult;
    protected final ColumnSource<?> externalResult;
    protected final byte onlyNullsSentinel;
    protected final byte nonUniqueSentinel;

    protected BaseByteChunkedUniqueOperator(
            // region Constructor
            // endregion Constructor
            String name, boolean countNullNaN, byte onlyNullsSentinel, byte nonUniqueSentinel) {
        this.name = name;
        this.countNullNaN = countNullNaN;
        this.onlyNullsSentinel = onlyNullsSentinel;
        this.nonUniqueSentinel = nonUniqueSentinel;
        // region EqualsConsistentAssignment
        // endregion EqualsConsistentAssignment

        // region ResultCreation
        this.internalResult = new ByteArraySource();
        // endregion ResultCreation
        // region ResultAssignment
        this.externalResult = internalResult;
        // endregion ResultAssignment
    }

    // region Bucketed Updates
    @NotNull
    protected BucketSsmDistinctContext getAndUpdateContext(Chunk<? extends Values> values,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, BucketedContext bucketedContext) {
        final BucketSsmDistinctContext context = (BucketSsmDistinctContext) bucketedContext;

        context.valueCopy.setSize(values.size());
        context.valueCopy.copyFromChunk(values, 0, 0, values.size());

        context.lengthCopy.setSize(length.size());
        context.lengthCopy.copyFromChunk(length, 0, 0, length.size());

        doCompactAndCount((WritableByteChunk<? extends Values>) context.valueCopy, context.counts,
                startPositions, context.lengthCopy, countNullNaN, countNullNaN);
        return context;
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctContext context = getAndUpdateContext(values, startPositions, length, bucketedContext);
        final WritableByteChunk<? extends Values> valueCopy = context.valueCopy.asWritableByteChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                stateModified.set(ii, false);
                continue;
            }
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addToState(destination, valueCopy, context.counts, startPosition, runLength));
        }
    }
    // endregion

    // region Singleton Updates
    @NotNull
    protected SsmDistinctContext getAndUpdateContext(Chunk<? extends Values> values,
            SingletonContext singletonContext) {
        final SsmDistinctContext context = (SsmDistinctContext) singletonContext;

        context.valueCopy.setSize(values.size());
        context.valueCopy.copyFromChunk(values, 0, 0, values.size());
        doCompactAndCount((WritableByteChunk<? extends Values>) context.valueCopy, context.counts,
                countNullNaN, countNullNaN);
        return context;
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final SsmDistinctContext context = getAndUpdateContext(values, singletonContext);
        if (context.valueCopy.size() == 0) {
            return false;
        }
        return addToState(destination, (WritableByteChunk<? extends Values>) context.valueCopy, context.counts, 0,
                context.valueCopy.size());
    }
    // endregion

    /**
     * Apply an addition of the compacted distinct {@code (value, count)} entries in {@code [start, start + len)} to
     * {@code destination}'s state.
     *
     * @return whether the result changed
     */
    protected abstract boolean addToState(long destination, WritableByteChunk<? extends Values> values,
            WritableIntChunk<ChunkLengths> counts, int start, int len);

    // region IterativeOperator / DistinctAggregationOperator
    @Override
    public void ensureCapacity(long tableSize) {
        internalResult.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.<String, ColumnSource<?>>singletonMap(name, externalResult);
    }

    @Override
    public void startTrackingPrevValues() {
        internalResult.startTrackingPrevValues();
    }
    // endregion

    // region Contexts
    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new BucketSsmDistinctContext(ChunkType.Byte, size);
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new SsmDistinctContext(ChunkType.Byte, size);
    }
    // endregion

    /**
     * Test two values for equality consistent with the ordering of the SSM; a state holds one entry for each class of
     * equal values.
     */
    protected static boolean eq(byte lhs, byte rhs) {
        // region equality function
        return ByteComparisons.eq(lhs, rhs);
        // endregion equality function
    }

    /**
     * Sorts {@code valueChunk}, compacts each run of equal values to one value, and sets each value's count in
     * {@code counts}; both chunks are resized to the number of distinct values.
     */
    protected void doCompactAndCount(WritableByteChunk<? extends Values> valueChunk,
            WritableIntChunk<ChunkLengths> counts, boolean countNull, boolean countNaN) {
        // region CompactAndCount
        ByteCompactKernel.compactAndCount(valueChunk, counts, countNull, countNaN);
        // endregion CompactAndCount
    }

    /**
     * Sorts and compacts each run of {@code valueChunk} given by {@code startPositions} and {@code lengths}, setting
     * each distinct value's count in {@code counts} and each run's distinct value count in {@code lengths}.
     */
    protected void doCompactAndCount(WritableByteChunk<? extends Values> valueChunk,
            WritableIntChunk<ChunkLengths> counts, IntChunk<ChunkPositions> startPositions,
            WritableIntChunk<ChunkLengths> lengths, boolean countNull, boolean countNaN) {
        // region CompactAndCountRuns
        ByteCompactKernel.compactAndCount(valueChunk, counts, startPositions, lengths, countNull, countNaN);
        // endregion CompactAndCountRuns
    }
}
