//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkedUniqueOperator and run "./gradlew replicateSegmentedSortedMultiset" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.ssmcountdistinct.unique;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.impl.by.RollupConstants;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.BucketSsmDistinctContext;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.IntSsmBackedSource;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.SsmDistinctContext;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.ssms.IntSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.compactmodifications.IntCompactModifications;
import io.deephaven.engine.table.impl.util.compact.IntCompactKernel;
import io.deephaven.util.compare.IntComparisons;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * This operator computes the single unique value of a particular aggregated state. If there are no values at all the
 * 'no value key' is used. If there are more than one values for the state, the 'non unique key' is used.
 *
 * <p>
 * A state holding exactly one distinct value is stored without a {@link IntSegmentedSortedMultiset SSM}: the value
 * lives in {@code internalResult} and its multiplicity in {@code singletonCount}. {@code singletonCount} encodes the
 * state: {@code NULL_LONG} or {@code 0} means empty, {@code > 0} means the single value in {@code internalResult} is
 * present that many times, and {@code -1} means the state holds two or more distinct values and an SSM in {@code ssms}
 * holds them. An SSM is created only when a state actually becomes non-unique, and is discarded when removals collapse
 * it back to a single distinct value (or empty).
 */
public class IntChunkedUniqueOperator implements IterativeChunkedAggregationOperator {
    private final String name;

    private final Supplier<SegmentedSortedMultiSet.RemoveContext> removeContextFactory;
    private final boolean countNullNaN;
    private final boolean exposeInternal;

    private final IntSsmBackedSource ssms;
    private final IntegerArraySource internalResult;
    private final LongArraySource singletonCount;
    private final ColumnSource<?> externalResult;
    private final int onlyNullsSentinel;
    private final int nonUniqueSentinel;

    public IntChunkedUniqueOperator(
            // region Constructor
            // endregion Constructor
            String name, boolean countNullNaN, boolean exposeInternal, int onlyNullsSentinel, int nonUniqueSentinel) {
        this.name = name;
        this.countNullNaN = countNullNaN;
        this.exposeInternal = exposeInternal;
        this.onlyNullsSentinel = onlyNullsSentinel;
        this.nonUniqueSentinel = nonUniqueSentinel;

        // region SsmCreation
        this.ssms = new IntSsmBackedSource();
        // endregion SsmCreation
        // region ResultCreation
        this.internalResult = new IntegerArraySource();
        // endregion ResultCreation
        this.singletonCount = new LongArraySource();
        // region ResultAssignment
        this.externalResult = internalResult;
        // endregion ResultAssignment

        removeContextFactory = SegmentedSortedMultiSet.makeRemoveContextFactory(SsmDistinctContext.NODE_SIZE);
    }

    // region Bucketed Updates
    @NotNull
    private BucketSsmDistinctContext getAndUpdateContext(Chunk<? extends Values> values,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, BucketedContext bucketedContext) {
        final BucketSsmDistinctContext context = (BucketSsmDistinctContext) bucketedContext;

        context.valueCopy.setSize(values.size());
        context.valueCopy.copyFromChunk(values, 0, 0, values.size());

        context.lengthCopy.setSize(length.size());
        context.lengthCopy.copyFromChunk(length, 0, 0, length.size());

        IntCompactKernel.compactAndCount((WritableIntChunk<? extends Values>) context.valueCopy, context.counts,
                startPositions, context.lengthCopy, countNullNaN, countNullNaN);
        return context;
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctContext context = getAndUpdateContext(values, startPositions, length, bucketedContext);
        final WritableIntChunk<? extends Values> valueCopy = context.valueCopy.asWritableIntChunk();
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

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctContext context = getAndUpdateContext(values, startPositions, length, bucketedContext);
        final SegmentedSortedMultiSet.RemoveContext removeContext = removeContextFactory.get();
        final WritableIntChunk<? extends Values> valueCopy = context.valueCopy.asWritableIntChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                stateModified.set(ii, false);
                continue;
            }
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii,
                    removeFromState(destination, removeContext, valueCopy, context.counts, startPosition, runLength));
        }
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctContext context = (BucketSsmDistinctContext) bucketedContext;
        // a modify produces one pre and one post value per row, so the two ranges share start positions and lengths
        context.valueCopy.setSize(preValues.size());
        context.valueCopy.copyFromChunk(preValues, 0, 0, preValues.size());
        context.postValues.setSize(postValues.size());
        context.postValues.copyFromChunk(postValues, 0, 0, postValues.size());

        final SegmentedSortedMultiSet.RemoveContext removeContext = removeContextFactory.get();
        final WritableIntChunk<? extends Values> preValueCopy =
                (WritableIntChunk<? extends Values>) context.valueCopy;
        final WritableIntChunk<? extends Values> postValueCopy =
                (WritableIntChunk<? extends Values>) context.postValues;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final int runLength = length.get(ii);
            if (runLength == 0) {
                stateModified.set(ii, false);
                continue;
            }

            // reduce the bucket's modify to its net effect: net removals in preValueCopy, net additions in
            // postValueCopy, with the unchanged overlap cancelled
            IntCompactModifications.compactAndCountModifications(preValueCopy, context.counts,
                    postValueCopy, context.postCounts, startPosition, runLength, startPosition, runLength,
                    countNullNaN, countNullNaN, context.removedSize, context.addedSize);

            stateModified.set(ii, modifyState(destination, removeContext, preValueCopy, context.counts, startPosition,
                    context.removedSize.get(), postValueCopy, context.postCounts, startPosition,
                    context.addedSize.get()));
        }
    }
    // endregion

    // region Singleton Updates
    @NotNull
    private SsmDistinctContext getAndUpdateContext(Chunk<? extends Values> values, SingletonContext singletonContext) {
        final SsmDistinctContext context = (SsmDistinctContext) singletonContext;

        context.valueCopy.setSize(values.size());
        context.valueCopy.copyFromChunk(values, 0, 0, values.size());
        IntCompactKernel.compactAndCount((WritableIntChunk<? extends Values>) context.valueCopy, context.counts,
                countNullNaN, countNullNaN);
        return context;
    }

    @NotNull
    private SsmDistinctContext getAndUpdateContext(Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, SingletonContext singletonContext) {
        final SsmDistinctContext context = (SsmDistinctContext) singletonContext;

        // a modify produces one pre and one post value per row, so the two ranges share a length
        final int length = preValues.size();
        context.valueCopy.setSize(length);
        context.valueCopy.copyFromChunk(preValues, 0, 0, length);
        context.postValues.setSize(length);
        context.postValues.copyFromChunk(postValues, 0, 0, length);
        IntCompactModifications.compactAndCountModifications(
                (WritableIntChunk<? extends Values>) context.valueCopy, context.counts,
                (WritableIntChunk<? extends Values>) context.postValues, context.postCounts,
                0, length, 0, length, countNullNaN, countNullNaN, context.removedSize, context.addedSize);
        return context;
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final SsmDistinctContext context = getAndUpdateContext(values, singletonContext);
        if (context.valueCopy.size() == 0) {
            return false;
        }
        return addToState(destination, (WritableIntChunk<? extends Values>) context.valueCopy, context.counts, 0,
                context.valueCopy.size());
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final SsmDistinctContext context = getAndUpdateContext(values, singletonContext);
        if (context.valueCopy.size() == 0) {
            return false;
        }
        return removeFromState(destination, context.removeContext,
                (WritableIntChunk<? extends Values>) context.valueCopy, context.counts, 0, context.valueCopy.size());
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        // reduce the modify to its net effect: valueCopy/counts hold the values to remove, postValues/postCounts the
        // values to add, with the unchanged overlap cancelled out
        final SsmDistinctContext context = getAndUpdateContext(preValues, postValues, singletonContext);
        return modifyState(destination, context.removeContext, (WritableIntChunk<? extends Values>) context.valueCopy,
                context.counts, 0, context.removedSize.get(), (WritableIntChunk<? extends Values>) context.postValues,
                context.postCounts, 0, context.addedSize.get());
    }
    // endregion

    // region State Machine
    /**
     * Apply an addition of the compacted distinct {@code (value, count)} entries in {@code [start, start + len)} to
     * {@code destination}'s state, transitioning empty -> unique -> non-unique (allocating an SSM) as needed.
     */
    private boolean addToState(long destination, WritableIntChunk<? extends Values> values,
            WritableIntChunk<ChunkLengths> counts, int start, int len) {
        final long priorState = singletonCount.getUnsafe(destination);
        if (isUnique(priorState)) {
            final int held = internalResult.getUnsafe(destination);
            if (len == 1 && IntComparisons.eq(values.get(start), held)) {
                // the single distinct value being added is the one we already hold; just bump its multiplicity. The
                // result value is unchanged; only the (internal) count moves.
                singletonCount.set(destination, priorState + counts.get(start));
                return exposeInternal;
            }
            // a second distinct value is arriving; materialize an SSM seeded with the held value, then add the rest
            final IntSegmentedSortedMultiset ssm = ssmForSlot(destination);
            ssm.insert(held, priorState);
            ssm.insert(values, counts, start, len);
            setNonUnique(destination);
            return exposeInternal || !IntComparisons.eq(held, nonUniqueSentinel);
        }
        if (isNonUnique(priorState)) {
            // already non-unique; the result value and count are unchanged regardless of what we insert
            ssmForSlot(destination).insert(values, counts, start, len);
            return false;
        }
        // empty -> non-empty is always a change to the result
        if (len == 1) {
            setUnique(destination, values.get(start), counts.get(start));
            return true;
        }
        ssmForSlot(destination).insert(values, counts, start, len);
        setNonUnique(destination);
        return true;
    }

    /**
     * Apply a removal of the compacted distinct {@code (value, count)} entries in {@code [start, start + len)} from
     * {@code destination}'s state, collapsing a non-unique SSM back to a unique value (or empty) as removals reduce its
     * cardinality. By contract every removed value is currently present.
     */
    private boolean removeFromState(long destination, SegmentedSortedMultiSet.RemoveContext removeContext,
            WritableIntChunk<? extends Values> values, WritableIntChunk<ChunkLengths> counts, int start, int len) {
        final long priorState = singletonCount.getUnsafe(destination);
        if (isUnique(priorState)) {
            // a unique state can only be asked to remove its one held value, and never more copies than it holds
            Assert.eq(len, "len", 1);
            Assert.assertion(IntComparisons.eq(values.get(start), internalResult.getUnsafe(destination)),
                    "values.get(start) == internalResult.getUnsafe(destination)");
            final long remaining = priorState - counts.get(start);
            Assert.geqZero(remaining, "remaining");
            if (remaining == 0) {
                setEmpty(destination);
                return true; // unique -> empty is always a change to the result
            }
            singletonCount.set(destination, remaining);
            return exposeInternal; // value unchanged; only the (internal) count moves
        }
        final IntSegmentedSortedMultiset ssm = ssmForSlot(destination);
        ssm.remove(removeContext, values, counts, start, len);
        return finishFromSsm(destination, ssm);
    }

    /**
     * Apply the net removals followed by the net additions of a modify to {@code destination}'s state, returning whether
     * the result changed. A non-unique state mutates its SSM in place (collapsing only once at the end); a unique/empty
     * state runs the removals then the additions through {@link #removeFromState} / {@link #addToState} (whose net is a
     * change iff either step is, since the net-removed and net-added value sets are disjoint).
     */
    private boolean modifyState(long destination, SegmentedSortedMultiSet.RemoveContext removeContext,
            WritableIntChunk<? extends Values> removedValues, WritableIntChunk<ChunkLengths> removedCounts,
            int removedStart, int removed,
            WritableIntChunk<? extends Values> addedValues, WritableIntChunk<ChunkLengths> addedCounts,
            int addedStart, int added) {
        if (isNonUnique(singletonCount.getUnsafe(destination))) {
            final IntSegmentedSortedMultiset ssm = ssmForSlot(destination);
            if (removed > 0) {
                ssm.remove(removeContext, removedValues, removedCounts, removedStart, removed);
            }
            if (added > 0) {
                ssm.insert(addedValues, addedCounts, addedStart, added);
            }
            return finishFromSsm(destination, ssm);
        }
        boolean changed = false;
        if (removed > 0) {
            changed = removeFromState(destination, removeContext, removedValues, removedCounts, removedStart, removed);
        }
        if (added > 0) {
            changed |= addToState(destination, addedValues, addedCounts, addedStart, added);
        }
        return changed;
    }

    /**
     * Resolve a non-unique state's representation after its SSM was mutated: discard the SSM and become empty or unique
     * if it has collapsed to zero or one distinct value, otherwise remain non-unique. Returns whether the result
     * changed.
     */
    private boolean finishFromSsm(long destination, IntSegmentedSortedMultiset ssm) {
        if (ssm.isEmpty()) {
            clearSsm(destination);
            setEmpty(destination);
            return true; // non-unique -> empty is always a change to the result
        }
        if (ssm.size() == 1) {
            final int value = ssm.get(0);
            final long count = ssm.getMaxCount();
            clearSsm(destination);
            setUnique(destination, value, count);
            // non-unique -> unique: the result value moves from the sentinel to the surviving value
            return exposeInternal || !IntComparisons.eq(nonUniqueSentinel, value);
        }
        // remains non-unique; the exposed value and count are unchanged
        return false;
    }

    private void setEmpty(long destination) {
        singletonCount.set(destination, 0L);
        internalResult.set(destination, onlyNullsSentinel);
    }

    private void setUnique(long destination, int value, long count) {
        singletonCount.set(destination, count);
        internalResult.set(destination, value);
    }

    private void setNonUnique(long destination) {
        singletonCount.set(destination, -1L);
        internalResult.set(destination, nonUniqueSentinel);
    }

    private static boolean isUnique(long sc) {
        return sc > 0;
    }

    private static boolean isNonUnique(long sc) {
        return sc != NULL_LONG && sc < 0;
    }
    // endregion

    // region IterativeOperator / DistinctAggregationOperator
    @Override
    public void ensureCapacity(long tableSize) {
        internalResult.ensureCapacity(tableSize);
        singletonCount.ensureCapacity(tableSize);
        ssms.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        if (exposeInternal) {
            final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
            columns.put(name, externalResult);
            columns.put(
                    name + RollupConstants.ROLLUP_DISTINCT_SSM_COUNT_COLUMN_ID + RollupConstants.ROLLUP_COLUMN_SUFFIX,
                    singletonCount);
            return columns;
        }

        return Collections.<String, ColumnSource<?>>singletonMap(name, externalResult);
    }

    @Override
    public void startTrackingPrevValues() {
        internalResult.startTrackingPrevValues();
        if (exposeInternal) {
            singletonCount.startTrackingPrevValues();
        }
    }

    // endregion

    // region Contexts
    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new BucketSsmDistinctContext(ChunkType.Int, size);
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new SsmDistinctContext(ChunkType.Int, size);
    }
    // endregion

    // region Private Helpers
    private IntSegmentedSortedMultiset ssmForSlot(long destination) {
        return ssms.getOrCreate(destination);
    }

    private void clearSsm(long destination) {
        ssms.clear(destination);
    }
    // endregion
}
