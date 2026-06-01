//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRollupUniqueOperator and run "./gradlew replicateSegmentedSortedMultiset" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.ssmcountdistinct.unique;

import io.deephaven.engine.table.impl.by.RollupConstants;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.*;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.compactmodifications.ByteCompactModifications;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.engine.table.impl.sources.ByteArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.*;
import io.deephaven.engine.table.impl.ssms.ByteSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import io.deephaven.engine.table.impl.util.compact.ByteCompactKernel;
import io.deephaven.util.compare.ByteComparisons;
import io.deephaven.util.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * This operator computes the single unique value of a particular aggregated state at the second and higher levels of a
 * rollup. Each constituent state below reports, via its value column and its {@code singletonCount} column, whether it
 * is empty ({@code 0}/{@code NULL_LONG}), uniquely a single value ({@code > 0}), or non-unique ({@code < 0}).
 *
 * <p>
 * This operator ignores the values of non-unique constituents, tracking only how many constituents are non-unique
 * ({@code nonUniqueCount}); a single such constituent forces this state's result to the non-unique sentinel. The values
 * of the unique constituents are gathered into a multiset: a single distinct value lives in {@code singletonValue} with
 * its count in {@code singletonCount}, and two or more distinct values are held in an SSM in {@code ssms}.
 *
 * <p>
 * {@code singletonCount} is also the column a grandparent reads, so it encodes both the result and the multiset's
 * count: {@code 0}/{@code NULL_LONG} empty, {@code > 0} the unique count, and {@code < 0} non-unique. When a non-unique
 * constituent forces a non-unique result while the multiset still holds a single value, that value's count {@code C} is
 * preserved as {@code -1 - C} (using the otherwise-unused {@code [NULL_LONG, -2]} range) so it survives until the
 * non-unique constituents are gone; an {@code ssms} entry, when present, means two or more distinct values.
 */
public class ByteRollupUniqueOperator implements IterativeChunkedAggregationOperator {
    private final String name;

    private final ByteSsmBackedSource ssms;
    private final ByteArraySource internalResult;
    private final LongArraySource singletonCount;
    private final ByteArraySource singletonValue;
    private final LongArraySource nonUniqueCount;
    private final ColumnSource<?> externalResult;
    private final ColumnSource<?> constituentSingletonCount;
    private final byte onlyNullsSentinel;
    private final byte nonUniqueSentinel;

    public ByteRollupUniqueOperator(
            // region Constructor
            // endregion Constructor
            String name,
            byte onlyNullsSentinel,
            byte nonUniqueSentinel,
            ColumnSource<?> constituentSingletonCount) {
        this.name = name;
        this.nonUniqueSentinel = nonUniqueSentinel;
        this.onlyNullsSentinel = onlyNullsSentinel;
        this.constituentSingletonCount = constituentSingletonCount;
        // region SsmCreation
        this.ssms = new ByteSsmBackedSource();
        // endregion SsmCreation
        // region ResultCreation
        this.internalResult = new ByteArraySource();
        // endregion ResultCreation
        this.singletonValue = new ByteArraySource();
        this.singletonCount = new LongArraySource();
        this.nonUniqueCount = new LongArraySource();
        // region ResultAssignment
        this.externalResult = internalResult;
        // endregion ResultAssignment
    }

    // region Bucketed Updates
    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final SsmUniqueRollupContext context = (SsmUniqueRollupContext) bucketedContext;
        final ByteChunk<? extends Values> valueChunk = values.asByteChunk();
        final WritableByteChunk<? extends Values> addValues = context.values.asWritableByteChunk();
        final MutableLong count = new MutableLong();
        final MutableObject<ByteSegmentedSortedMultiset> ssmHolder = new MutableObject<>();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            final long priorState = singletonCount.getUnsafe(destination);
            final byte priorValue = internalResult.getUnsafe(destination);
            loadState(destination, count, ssmHolder);
            int addCount = 0;
            long nonUniqueDelta = 0;
            for (int kk = startPosition; kk < startPosition + runLength; ++kk) {
                final long sc = constituentSingletonCount.getLong(inputRowKeys.get(kk));
                if (sc > 0) {
                    addValues.set(addCount++, valueChunk.get(kk));
                } else if (isNonUniqueState(sc)) {
                    nonUniqueDelta++;
                }
            }
            compactAndApplyAdds(destination, addValues, addCount, context.counts, count, ssmHolder);
            applyNonUniqueDelta(destination, nonUniqueDelta);
            stateModified.set(ii,
                    finalizeState(destination, priorState, priorValue, count.get(), ssmHolder.getValue()));
        }
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final SsmUniqueRollupContext context = (SsmUniqueRollupContext) bucketedContext;
        final ByteChunk<? extends Values> valueChunk = values.asByteChunk();
        final WritableByteChunk<? extends Values> removeValues = context.values.asWritableByteChunk();
        final MutableLong count = new MutableLong();
        final MutableObject<ByteSegmentedSortedMultiset> ssmHolder = new MutableObject<>();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            final long priorState = singletonCount.getUnsafe(destination);
            final byte priorValue = internalResult.getUnsafe(destination);
            loadState(destination, count, ssmHolder);
            int removeCount = 0;
            long nonUniqueDelta = 0;
            for (int kk = startPosition; kk < startPosition + runLength; ++kk) {
                // the removed constituents are gone from the current table, so read their prior singletonCount
                final long sc = constituentSingletonCount.getPrevLong(inputRowKeys.get(kk));
                if (sc > 0) {
                    removeValues.set(removeCount++, valueChunk.get(kk));
                } else if (isNonUniqueState(sc)) {
                    nonUniqueDelta--;
                }
            }
            compactAndApplyRemoves(destination, removeValues, removeCount, context.counts, context.removeContext, count,
                    ssmHolder);
            applyNonUniqueDelta(destination, nonUniqueDelta);
            stateModified.set(ii,
                    finalizeState(destination, priorState, priorValue, count.get(), ssmHolder.getValue()));
        }
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final SsmUniqueRollupContext context = (SsmUniqueRollupContext) bucketedContext;
        final ByteChunk<? extends Values> prevValueChunk = preValues.asByteChunk();
        final ByteChunk<? extends Values> postValueChunk = postValues.asByteChunk();
        final WritableByteChunk<? extends Values> removeValues = context.values.asWritableByteChunk();
        final WritableByteChunk<? extends Values> addValues = context.postValues.asWritableByteChunk();
        final MutableLong count = new MutableLong();
        final MutableObject<ByteSegmentedSortedMultiset> ssmHolder = new MutableObject<>();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            final long priorState = singletonCount.getUnsafe(destination);
            final byte priorValue = internalResult.getUnsafe(destination);
            loadState(destination, count, ssmHolder);
            long nonUniqueDelta = 0;

            // gather each constituent's previous contribution as a removal and its current one as an addition
            int removeCount = 0;
            int addCount = 0;
            for (int kk = startPosition; kk < startPosition + runLength; ++kk) {
                final long rowKey = postShiftRowKeys.get(kk);
                final long prevSc = constituentSingletonCount.getPrevLong(rowKey);
                if (prevSc > 0) {
                    removeValues.set(removeCount++, prevValueChunk.get(kk));
                } else if (isNonUniqueState(prevSc)) {
                    nonUniqueDelta--;
                }
                final long sc = constituentSingletonCount.getLong(rowKey);
                if (sc > 0) {
                    addValues.set(addCount++, postValueChunk.get(kk));
                } else if (isNonUniqueState(sc)) {
                    nonUniqueDelta++;
                }
            }
            // net the two so values unchanged across the modify cancel out, then apply the surviving removals/additions
            ByteCompactModifications.compactAndCountModifications(removeValues, context.counts, addValues,
                    context.postCounts, 0, removeCount, 0, addCount, true, true, context.removedSize,
                    context.addedSize);
            applyRemoves(destination, removeValues, context.removedSize.get(), context.counts, context.removeContext,
                    count, ssmHolder);
            applyAdds(destination, addValues, context.addedSize.get(), context.postCounts, count, ssmHolder);

            applyNonUniqueDelta(destination, nonUniqueDelta);
            stateModified.set(ii,
                    finalizeState(destination, priorState, priorValue, count.get(), ssmHolder.getValue()));
        }
    }
    // endregion

    // region Singleton Updates
    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final SsmUniqueRollupContext context = (SsmUniqueRollupContext) singletonContext;
        final ByteChunk<? extends Values> valueChunk = values.asByteChunk();
        final WritableByteChunk<? extends Values> addValues = context.values.asWritableByteChunk();
        final long priorState = singletonCount.getUnsafe(destination);
        final byte priorValue = internalResult.getUnsafe(destination);
        final MutableLong count = new MutableLong();
        final MutableObject<ByteSegmentedSortedMultiset> ssmHolder = new MutableObject<>();
        loadState(destination, count, ssmHolder);
        int addCount = 0;
        long nonUniqueDelta = 0;
        for (int kk = 0; kk < chunkSize; ++kk) {
            final long sc = constituentSingletonCount.getLong(inputRowKeys.get(kk));
            if (sc > 0) {
                addValues.set(addCount++, valueChunk.get(kk));
            } else if (isNonUniqueState(sc)) {
                nonUniqueDelta++;
            }
        }
        compactAndApplyAdds(destination, addValues, addCount, context.counts, count, ssmHolder);
        applyNonUniqueDelta(destination, nonUniqueDelta);
        return finalizeState(destination, priorState, priorValue, count.get(), ssmHolder.getValue());
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final SsmUniqueRollupContext context = (SsmUniqueRollupContext) singletonContext;
        final ByteChunk<? extends Values> valueChunk = values.asByteChunk();
        final WritableByteChunk<? extends Values> removeValues = context.values.asWritableByteChunk();
        final long priorState = singletonCount.getUnsafe(destination);
        final byte priorValue = internalResult.getUnsafe(destination);
        final MutableLong count = new MutableLong();
        final MutableObject<ByteSegmentedSortedMultiset> ssmHolder = new MutableObject<>();
        loadState(destination, count, ssmHolder);
        int removeCount = 0;
        long nonUniqueDelta = 0;
        for (int kk = 0; kk < chunkSize; ++kk) {
            // the removed constituents are gone from the current table, so read their prior singletonCount
            final long sc = constituentSingletonCount.getPrevLong(inputRowKeys.get(kk));
            if (sc > 0) {
                removeValues.set(removeCount++, valueChunk.get(kk));
            } else if (isNonUniqueState(sc)) {
                nonUniqueDelta--;
            }
        }
        compactAndApplyRemoves(destination, removeValues, removeCount, context.counts, context.removeContext, count,
                ssmHolder);
        applyNonUniqueDelta(destination, nonUniqueDelta);
        return finalizeState(destination, priorState, priorValue, count.get(), ssmHolder.getValue());
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        final SsmUniqueRollupContext context = (SsmUniqueRollupContext) singletonContext;
        final ByteChunk<? extends Values> prevValueChunk = preValues.asByteChunk();
        final ByteChunk<? extends Values> postValueChunk = postValues.asByteChunk();
        final WritableByteChunk<? extends Values> removeValues = context.values.asWritableByteChunk();
        final WritableByteChunk<? extends Values> addValues = context.postValues.asWritableByteChunk();
        final long priorState = singletonCount.getUnsafe(destination);
        final byte priorValue = internalResult.getUnsafe(destination);
        final MutableLong count = new MutableLong();
        final MutableObject<ByteSegmentedSortedMultiset> ssmHolder = new MutableObject<>();
        loadState(destination, count, ssmHolder);
        long nonUniqueDelta = 0;

        // gather each constituent's previous contribution as a removal and its current one as an addition
        int removeCount = 0;
        int addCount = 0;
        for (int kk = 0; kk < chunkSize; ++kk) {
            final long rowKey = postShiftRowKeys.get(kk);
            final long prevSc = constituentSingletonCount.getPrevLong(rowKey);
            if (prevSc > 0) {
                removeValues.set(removeCount++, prevValueChunk.get(kk));
            } else if (isNonUniqueState(prevSc)) {
                nonUniqueDelta--;
            }
            final long sc = constituentSingletonCount.getLong(rowKey);
            if (sc > 0) {
                addValues.set(addCount++, postValueChunk.get(kk));
            } else if (isNonUniqueState(sc)) {
                nonUniqueDelta++;
            }
        }
        // net the two so values unchanged across the modify cancel out, then apply the surviving removals/additions
        ByteCompactModifications.compactAndCountModifications(removeValues, context.counts, addValues,
                context.postCounts, 0, removeCount, 0, addCount, true, true, context.removedSize, context.addedSize);
        applyRemoves(destination, removeValues, context.removedSize.get(), context.counts, context.removeContext, count,
                ssmHolder);
        applyAdds(destination, addValues, context.addedSize.get(), context.postCounts, count, ssmHolder);

        applyNonUniqueDelta(destination, nonUniqueDelta);
        return finalizeState(destination, priorState, priorValue, count.get(), ssmHolder.getValue());
    }
    // endregion

    // region State Machine
    /**
     * Load {@code destination}'s multiset state once into the supplied holders so a run of constituents for the same
     * destination need not re-read {@code ssms} and {@code singletonCount}: the SSM (when two or more distinct values
     * are present) goes into {@code ssmHolder}, otherwise the singleton count goes into {@code count}.
     */
    private void loadState(long destination, MutableLong count,
            MutableObject<ByteSegmentedSortedMultiset> ssmHolder) {
        final long sc = singletonCount.getUnsafe(destination);
        if (isNonUniqueState(sc)) {
            // only a non-unique encoding can be backed by an SSM holding two or more distinct values
            final ByteSegmentedSortedMultiset ssm = ssms.getCurrentSsm(destination);
            ssmHolder.setValue(ssm);
            count.set(ssm == null ? -1 - sc : 0);
            return;
        }
        ssmHolder.setValue(null);
        count.set(sc == NULL_LONG ? 0 : sc);
    }

    /**
     * Compact {@code addCount} raw values (one per contributing constituent) in place into distinct values +
     * {@code counts} and add them via {@link #applyAdds}. Used by the add-only paths; a modify nets its values with
     * {@link ByteCompactModifications} instead and calls {@link #applyAdds} directly.
     */
    private void compactAndApplyAdds(long destination, WritableByteChunk<? extends Values> values, int addCount,
            WritableIntChunk<ChunkLengths> counts, MutableLong count,
            MutableObject<ByteSegmentedSortedMultiset> ssmHolder) {
        if (addCount == 0) {
            return;
        }
        values.setSize(addCount);
        ByteCompactKernel.compactAndCount(values, counts, true, true);
        applyAdds(destination, values, values.size(), counts, count, ssmHolder);
    }

    /**
     * Add a destination's compacted distinct unique constituent values to its multiset (held in {@code count} /
     * {@code ssmHolder}) in a single bulk operation. {@code values} / {@code counts} hold the first
     * {@code distinctCount} distinct values and their occurrence counts, transitioning empty -> unique -> non-unique
     * (allocating an SSM) as a second distinct value appears. {@link #finalizeState} writes the resulting encoding to
     * {@code singletonCount} afterward.
     */
    private void applyAdds(long destination, WritableByteChunk<? extends Values> values, int distinctCount,
            WritableIntChunk<ChunkLengths> counts, MutableLong count,
            MutableObject<ByteSegmentedSortedMultiset> ssmHolder) {
        if (distinctCount == 0) {
            return;
        }
        final ByteSegmentedSortedMultiset ssm = ssmHolder.getValue();
        if (ssm != null) {
            ssm.insert(values, counts, 0, distinctCount);
            return;
        }
        if (count.get() == 0) {
            // empty
            if (distinctCount == 1) {
                singletonValue.set(destination, values.get(0));
                count.set(counts.get(0));
                return;
            }
            final ByteSegmentedSortedMultiset newSsm = ssmForSlot(destination);
            newSsm.insert(values, counts, 0, distinctCount);
            ssmHolder.setValue(newSsm);
            return;
        }
        // singleton: a single held value with a positive count
        final byte held = singletonValue.getUnsafe(destination);
        if (distinctCount == 1 && ByteComparisons.eq(values.get(0), held)) {
            count.add(counts.get(0));
            return;
        }
        // a second distinct value is arriving; materialize an SSM seeded with the held value
        final ByteSegmentedSortedMultiset newSsm = ssmForSlot(destination);
        newSsm.insert(held, count.get());
        newSsm.insert(values, counts, 0, distinctCount);
        ssmHolder.setValue(newSsm);
        clearSingletonValue(destination);
    }

    /**
     * Compact {@code removeCount} raw values (one per departing constituent) in place into distinct values +
     * {@code counts} and remove them via {@link #applyRemoves}. Used by the remove-only paths; a modify nets its values
     * with {@link ByteCompactModifications} instead and calls {@link #applyRemoves} directly.
     */
    private void compactAndApplyRemoves(long destination, WritableByteChunk<? extends Values> values, int removeCount,
            WritableIntChunk<ChunkLengths> counts, SegmentedSortedMultiSet.RemoveContext removeContext,
            MutableLong count, MutableObject<ByteSegmentedSortedMultiset> ssmHolder) {
        if (removeCount == 0) {
            return;
        }
        values.setSize(removeCount);
        ByteCompactKernel.compactAndCount(values, counts, true, true);
        applyRemoves(destination, values, values.size(), counts, removeContext, count, ssmHolder);
    }

    /**
     * Remove a destination's compacted distinct unique constituent values from its multiset (held in {@code count} /
     * {@code ssmHolder}) in a single bulk operation. {@code values} / {@code counts} hold the first
     * {@code distinctCount} distinct values and their occurrence counts, collapsing a non-unique SSM back to a unique
     * value (or empty) as its cardinality drops. The values must currently be present. {@link #finalizeState} writes
     * the resulting encoding to {@code singletonCount} afterward.
     */
    private void applyRemoves(long destination, WritableByteChunk<? extends Values> values, int distinctCount,
            WritableIntChunk<ChunkLengths> counts, SegmentedSortedMultiSet.RemoveContext removeContext,
            MutableLong count, MutableObject<ByteSegmentedSortedMultiset> ssmHolder) {
        if (distinctCount == 0) {
            return;
        }
        final ByteSegmentedSortedMultiset ssm = ssmHolder.getValue();
        if (ssm != null) {
            ssm.remove(removeContext, values, counts, 0, distinctCount);
            if (ssm.isEmpty()) {
                clearSsm(destination);
                ssmHolder.setValue(null);
                count.set(0);
            } else if (ssm.size() == 1) {
                singletonValue.set(destination, ssm.get(0));
                count.set(ssm.getMaxCount());
                clearSsm(destination);
                ssmHolder.setValue(null);
            }
            return;
        }
        // a singleton multiset holds a single value, so every removal must target it
        count.set(count.get() - counts.get(0));
        if (count.get() == 0) {
            clearSingletonValue(destination);
        }
    }

    private void applyNonUniqueDelta(long destination, long nonUniqueDelta) {
        if (nonUniqueDelta != 0) {
            final long prior = nonUniqueCount.getUnsafe(destination);
            nonUniqueCount.set(destination, (prior == NULL_LONG ? 0 : prior) + nonUniqueDelta);
        }
    }

    /**
     * Derive the exposed encoding of {@code internalResult} and {@code singletonCount} from the multiset (the supplied
     * {@code count} / {@code ssm}) and {@code nonUniqueCount}, writing each only when it actually changes from
     * ({@code priorValue}, {@code priorState}), and returning whether either changed. A grandparent reads
     * {@code singletonCount} as: {@code > 0} unique (the count), {@code 0}/{@code NULL_LONG} empty, {@code < 0}
     * non-unique. When a non-unique constituent forces a non-unique result while the multiset is still a single value,
     * the count is preserved as {@code -1 - count} so it survives until the non-unique constituents are gone.
     */
    private boolean finalizeState(long destination, long priorState, byte priorValue, long count,
            ByteSegmentedSortedMultiset ssm) {
        final byte newValue;
        final long newCount;
        if (ssm != null) {
            // two or more distinct values
            newValue = nonUniqueSentinel;
            newCount = -1L;
        } else {
            final long nuc = nonUniqueCount.getUnsafe(destination);
            if (nuc != NULL_LONG && nuc > 0) {
                newValue = nonUniqueSentinel;
                newCount = -1 - count;
            } else if (count > 0) {
                newValue = singletonValue.getUnsafe(destination);
                newCount = count;
            } else {
                newValue = onlyNullsSentinel;
                newCount = 0L;
            }
        }
        boolean changed = false;
        if (!ByteComparisons.eq(newValue, priorValue)) {
            internalResult.set(destination, newValue);
            changed = true;
        }
        if (newCount != priorState) {
            singletonCount.set(destination, newCount);
            changed = true;
        }
        return changed;
    }

    /**
     * Forget {@code destination}'s former singleton value now that two or more distinct values live in its SSM. A no-op
     * for the primitive specializations; the object specialization nulls the reference so it is not retained.
     */
    private void clearSingletonValue(long destination) {
        // region clearSingletonValue
        // endregion clearSingletonValue
    }

    private static boolean isNonUniqueState(long stateCount) {
        return stateCount != NULL_LONG && stateCount < 0;
    }
    // endregion

    // region IterativeOperator / DistinctAggregationOperator
    @Override
    public void ensureCapacity(long tableSize) {
        internalResult.ensureCapacity(tableSize);
        singletonCount.ensureCapacity(tableSize);
        singletonValue.ensureCapacity(tableSize);
        nonUniqueCount.ensureCapacity(tableSize);
        ssms.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        columns.put(name, externalResult);
        columns.put(name + RollupConstants.ROLLUP_DISTINCT_SSM_COUNT_COLUMN_ID + RollupConstants.ROLLUP_COLUMN_SUFFIX,
                singletonCount);
        return columns;
    }

    @Override
    public void startTrackingPrevValues() {
        internalResult.startTrackingPrevValues();
        singletonCount.startTrackingPrevValues();
    }
    // endregion

    // region Contexts
    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new SsmUniqueRollupContext(ChunkType.Byte, size);
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new SsmUniqueRollupContext(ChunkType.Byte, size);
    }
    // endregion

    // region Private Helpers
    private ByteSegmentedSortedMultiset ssmForSlot(long destination) {
        return ssms.getOrCreate(destination);
    }

    private void clearSsm(long destination) {
        ssms.clear(destination);
    }
    // endregion
}
