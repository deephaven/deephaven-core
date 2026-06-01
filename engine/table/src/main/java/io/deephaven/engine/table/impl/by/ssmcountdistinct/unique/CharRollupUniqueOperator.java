//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.ssmcountdistinct.unique;

import io.deephaven.engine.table.impl.by.RollupConstants;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.*;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.engine.table.impl.sources.CharacterArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.*;
import io.deephaven.engine.table.impl.ssms.CharSegmentedSortedMultiset;
import io.deephaven.util.compare.CharComparisons;
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
public class CharRollupUniqueOperator implements IterativeChunkedAggregationOperator {
    private final String name;

    private final CharSsmBackedSource ssms;
    private final CharacterArraySource internalResult;
    private final LongArraySource singletonCount;
    private final CharacterArraySource singletonValue;
    private final LongArraySource nonUniqueCount;
    private final ColumnSource<?> externalResult;
    private final ColumnSource<?> constituentSingletonCount;
    private final char onlyNullsSentinel;
    private final char nonUniqueSentinel;

    public CharRollupUniqueOperator(
            // region Constructor
            // endregion Constructor
            String name,
            char onlyNullsSentinel,
            char nonUniqueSentinel,
            ColumnSource<?> constituentSingletonCount) {
        this.name = name;
        this.nonUniqueSentinel = nonUniqueSentinel;
        this.onlyNullsSentinel = onlyNullsSentinel;
        this.constituentSingletonCount = constituentSingletonCount;
        // region SsmCreation
        this.ssms = new CharSsmBackedSource();
        // endregion SsmCreation
        // region ResultCreation
        this.internalResult = new CharacterArraySource();
        // endregion ResultCreation
        this.singletonValue = new CharacterArraySource();
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
        final CharChunk<? extends Values> valueChunk = values.asCharChunk();
        final MutableLong count = new MutableLong();
        final MutableObject<CharSegmentedSortedMultiset> ssmHolder = new MutableObject<>();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            final long priorState = singletonCount.getUnsafe(destination);
            final char priorValue = internalResult.getUnsafe(destination);
            loadState(destination, count, ssmHolder);
            long nonUniqueDelta = 0;
            for (int kk = startPosition; kk < startPosition + runLength; ++kk) {
                final long sc = constituentSingletonCount.getLong(inputRowKeys.get(kk));
                if (sc > 0) {
                    addValueToMultiset(destination, valueChunk.get(kk), count, ssmHolder);
                } else if (isNonUniqueState(sc)) {
                    nonUniqueDelta++;
                }
            }
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
        final CharChunk<? extends Values> valueChunk = values.asCharChunk();
        final MutableLong count = new MutableLong();
        final MutableObject<CharSegmentedSortedMultiset> ssmHolder = new MutableObject<>();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            final long priorState = singletonCount.getUnsafe(destination);
            final char priorValue = internalResult.getUnsafe(destination);
            loadState(destination, count, ssmHolder);
            long nonUniqueDelta = 0;
            for (int kk = startPosition; kk < startPosition + runLength; ++kk) {
                // the removed constituents are gone from the current table, so read their prior singletonCount
                final long sc = constituentSingletonCount.getPrevLong(inputRowKeys.get(kk));
                if (sc > 0) {
                    removeValueFromMultiset(destination, valueChunk.get(kk), count, ssmHolder);
                } else if (isNonUniqueState(sc)) {
                    nonUniqueDelta--;
                }
            }
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
        final CharChunk<? extends Values> prevValueChunk = preValues.asCharChunk();
        final CharChunk<? extends Values> postValueChunk = postValues.asCharChunk();
        final MutableLong count = new MutableLong();
        final MutableObject<CharSegmentedSortedMultiset> ssmHolder = new MutableObject<>();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            final long priorState = singletonCount.getUnsafe(destination);
            final char priorValue = internalResult.getUnsafe(destination);
            loadState(destination, count, ssmHolder);
            long nonUniqueDelta = 0;
            for (int kk = startPosition; kk < startPosition + runLength; ++kk) {
                final long rowKey = postShiftRowKeys.get(kk);
                // reverse the constituent's previous contribution, then apply its current one
                final long prevSc = constituentSingletonCount.getPrevLong(rowKey);
                if (prevSc > 0) {
                    removeValueFromMultiset(destination, prevValueChunk.get(kk), count, ssmHolder);
                } else if (isNonUniqueState(prevSc)) {
                    nonUniqueDelta--;
                }
                final long sc = constituentSingletonCount.getLong(rowKey);
                if (sc > 0) {
                    addValueToMultiset(destination, postValueChunk.get(kk), count, ssmHolder);
                } else if (isNonUniqueState(sc)) {
                    nonUniqueDelta++;
                }
            }
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
        final CharChunk<? extends Values> valueChunk = values.asCharChunk();
        final long priorState = singletonCount.getUnsafe(destination);
        final char priorValue = internalResult.getUnsafe(destination);
        final MutableLong count = new MutableLong();
        final MutableObject<CharSegmentedSortedMultiset> ssmHolder = new MutableObject<>();
        loadState(destination, count, ssmHolder);
        long nonUniqueDelta = 0;
        for (int kk = 0; kk < chunkSize; ++kk) {
            final long sc = constituentSingletonCount.getLong(inputRowKeys.get(kk));
            if (sc > 0) {
                addValueToMultiset(destination, valueChunk.get(kk), count, ssmHolder);
            } else if (isNonUniqueState(sc)) {
                nonUniqueDelta++;
            }
        }
        applyNonUniqueDelta(destination, nonUniqueDelta);
        return finalizeState(destination, priorState, priorValue, count.get(), ssmHolder.getValue());
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final CharChunk<? extends Values> valueChunk = values.asCharChunk();
        final long priorState = singletonCount.getUnsafe(destination);
        final char priorValue = internalResult.getUnsafe(destination);
        final MutableLong count = new MutableLong();
        final MutableObject<CharSegmentedSortedMultiset> ssmHolder = new MutableObject<>();
        loadState(destination, count, ssmHolder);
        long nonUniqueDelta = 0;
        for (int kk = 0; kk < chunkSize; ++kk) {
            // the removed constituents are gone from the current table, so read their prior singletonCount
            final long sc = constituentSingletonCount.getPrevLong(inputRowKeys.get(kk));
            if (sc > 0) {
                removeValueFromMultiset(destination, valueChunk.get(kk), count, ssmHolder);
            } else if (isNonUniqueState(sc)) {
                nonUniqueDelta--;
            }
        }
        applyNonUniqueDelta(destination, nonUniqueDelta);
        return finalizeState(destination, priorState, priorValue, count.get(), ssmHolder.getValue());
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        final CharChunk<? extends Values> prevValueChunk = preValues.asCharChunk();
        final CharChunk<? extends Values> postValueChunk = postValues.asCharChunk();
        final long priorState = singletonCount.getUnsafe(destination);
        final char priorValue = internalResult.getUnsafe(destination);
        final MutableLong count = new MutableLong();
        final MutableObject<CharSegmentedSortedMultiset> ssmHolder = new MutableObject<>();
        loadState(destination, count, ssmHolder);
        long nonUniqueDelta = 0;
        for (int kk = 0; kk < chunkSize; ++kk) {
            final long rowKey = postShiftRowKeys.get(kk);
            final long prevSc = constituentSingletonCount.getPrevLong(rowKey);
            if (prevSc > 0) {
                removeValueFromMultiset(destination, prevValueChunk.get(kk), count, ssmHolder);
            } else if (isNonUniqueState(prevSc)) {
                nonUniqueDelta--;
            }
            final long sc = constituentSingletonCount.getLong(rowKey);
            if (sc > 0) {
                addValueToMultiset(destination, postValueChunk.get(kk), count, ssmHolder);
            } else if (isNonUniqueState(sc)) {
                nonUniqueDelta++;
            }
        }
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
            MutableObject<CharSegmentedSortedMultiset> ssmHolder) {
        final long sc = singletonCount.getUnsafe(destination);
        if (isNonUniqueState(sc)) {
            // only a non-unique encoding can be backed by an SSM holding two or more distinct values
            final CharSegmentedSortedMultiset ssm = ssms.getCurrentSsm(destination);
            ssmHolder.setValue(ssm);
            count.set(ssm == null ? -1 - sc : 0);
            return;
        }
        ssmHolder.setValue(null);
        count.set(sc == NULL_LONG ? 0 : sc);
    }

    /**
     * Add one occurrence of {@code value} (one unique constituent holding it) to the destination's multiset (held in
     * {@code count} / {@code ssmHolder}), transitioning empty -> unique -> non-unique (allocating an SSM) as a second
     * distinct value arrives. {@link #finalizeState} writes the resulting encoding to {@code singletonCount} afterward.
     */
    private void addValueToMultiset(long destination, char value, MutableLong count,
            MutableObject<CharSegmentedSortedMultiset> ssmHolder) {
        final CharSegmentedSortedMultiset ssm = ssmHolder.getValue();
        if (ssm != null) {
            ssm.insert(value, 1);
            return;
        }
        if (count.get() == 0) {
            singletonValue.set(destination, value);
            count.set(1);
            return;
        }
        final char held = singletonValue.getUnsafe(destination);
        if (CharComparisons.eq(value, held)) {
            count.increment();
            return;
        }
        // a second distinct value is arriving; materialize an SSM holding both
        final CharSegmentedSortedMultiset newSsm = ssmForSlot(destination);
        newSsm.insert(held, count.get());
        newSsm.insert(value, 1);
        ssmHolder.setValue(newSsm);
        clearSingletonValue(destination);
    }

    /**
     * Remove one occurrence of {@code value} from the destination's multiset (held in {@code count} / {@code
     * ssmHolder}), collapsing a non-unique SSM back to a unique value (or empty) as its cardinality drops. The value
     * must currently be present.
     */
    private void removeValueFromMultiset(long destination, char value, MutableLong count,
            MutableObject<CharSegmentedSortedMultiset> ssmHolder) {
        final CharSegmentedSortedMultiset ssm = ssmHolder.getValue();
        if (ssm != null) {
            ssm.remove(value, 1);
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
        // a singleton multiset can only be asked to remove its single held value
        count.decrement();
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
    private boolean finalizeState(long destination, long priorState, char priorValue, long count,
            CharSegmentedSortedMultiset ssm) {
        final char newValue;
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
        if (!CharComparisons.eq(newValue, priorValue)) {
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
        return new BucketSsmDistinctRollupContext(ChunkType.Char, size);
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new SsmDistinctRollupContext(ChunkType.Char);
    }
    // endregion

    // region Private Helpers
    private CharSegmentedSortedMultiset ssmForSlot(long destination) {
        return ssms.getOrCreate(destination);
    }

    private void clearSsm(long destination) {
        ssms.clear(destination);
    }
    // endregion
}
