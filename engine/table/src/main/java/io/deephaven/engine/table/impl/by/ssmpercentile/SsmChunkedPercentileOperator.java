//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.ssmpercentile;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableListener;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.chunk.*;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper;
import io.deephaven.engine.table.impl.util.compact.CompactKernel;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Percentile operator backed by two {@link SegmentedSortedMultiSet SSMs} per destination: the low set holds the values
 * at or below the percentile, and the high set holds the rest.
 * <p>
 * Values are not inserted into or removed from the SSMs as each chunk arrives. Instead, each destination stages the
 * values added to it and the values removed from it (a modification stages its previous value as a removal and its new
 * value as an addition). When the initial state or a cycle's updates are propagated, each touched destination compacts
 * its staged values into sorted, counted runs, nets the removals against the additions so that a value both removed and
 * added touches neither SSM, applies one pivoted removal and one pivoted insertion, and then sets its result. Every
 * destination that receives values reports a modification, whether or not its percentile changes.
 */
public class SsmChunkedPercentileOperator implements IterativeChunkedAggregationOperator {
    private static final int NODE_SIZE =
            Configuration.getInstance().getIntegerWithDefault("SsmChunkedMinMaxOperator.nodeSize", 4096);
    private final WritableColumnSource internalResult;
    private final ColumnSource externalResult;
    /**
     * Even slots hold the low values, odd slots hold the high values.
     */
    private final ObjectArraySource<SegmentedSortedMultiSet> ssms;
    private final String name;
    private final CompactKernel compactAndCountKernel;
    private final NetChangeKernel netChangeKernel;
    private final Supplier<SegmentedSortedMultiSet> ssmFactory;
    private final ChunkType chunkType;
    private final PercentileTypeHelper percentileTypeHelper;

    /**
     * The values added to each destination since its staged values were last applied, or null if there are none.
     */
    private final ObjectArraySource<WritableChunk<Values>> stagedAdds;
    /**
     * The values removed from each destination since its staged values were last applied, or null if there are none.
     */
    private final ObjectArraySource<WritableChunk<Values>> stagedRemoves;
    /**
     * The destinations with staged values, in the order they were first staged.
     */
    private final IntegerArraySource touchedDestinations = new IntegerArraySource();
    private int touchedCount;

    /**
     * @param type the data type of the values
     * @param equalsConsistent true when values of the type compare equal exactly when they are equal (see
     *        {@link BinarySearchKernelHelper#compareConsistentWithEquality(Class)}), which selects the
     *        EqualsConsistentObject set and compact kernel that test Object equality with {@code equals}; other chunk
     *        types ignore it
     * @param percentile the percentile to compute
     * @param averageEvenlyDivided see {@link io.deephaven.api.agg.spec.AggSpecPercentile#averageEvenlyDivided()}
     * @param name the name of the result column
     */
    public SsmChunkedPercentileOperator(Class<?> type, boolean equalsConsistent, double percentile,
            boolean averageEvenlyDivided, String name) {
        this.name = name;
        this.ssms = new ObjectArraySource<>(SegmentedSortedMultiSet.class);
        final boolean isInstant = type == Instant.class;
        if (isInstant) {
            chunkType = ChunkType.Long;
        } else {
            chunkType = ChunkType.fromElementType(type);
        }
        if (isInstant) {
            internalResult = new LongArraySource();
            // noinspection unchecked
            externalResult = new LongAsInstantColumnSource(internalResult);
            averageEvenlyDivided = false;
        } else {
            if (averageEvenlyDivided) {
                switch (chunkType) {
                    case Int:
                    case Long:
                    case Double:
                        internalResult = new DoubleArraySource();
                        break;
                    case Float:
                        internalResult = new FloatArraySource();
                        break;
                    default:
                        // for things that are not int, long, double, or float we do not actually average the median;
                        // we just do the standard 50-%tile thing. It might be worth defining this to be friendlier.
                        internalResult = ArrayBackedColumnSource.getMemoryColumnSource(0, type);
                }
            } else {
                internalResult = ArrayBackedColumnSource.getMemoryColumnSource(0, type);
            }
            externalResult = internalResult;
        }
        compactAndCountKernel = CompactKernel.makeCompact(chunkType, equalsConsistent);
        netChangeKernel = NetChangeKernel.make(chunkType);
        ssmFactory = SegmentedSortedMultiSet.makeFactory(chunkType, NODE_SIZE, type, equalsConsistent);
        // noinspection unchecked,rawtypes
        stagedAdds = new ObjectArraySource<>((Class) WritableChunk.class);
        // noinspection unchecked,rawtypes
        stagedRemoves = new ObjectArraySource<>((Class) WritableChunk.class);
        percentileTypeHelper = makeTypeHelper(chunkType, type, percentile, averageEvenlyDivided, internalResult);
    }

    private static PercentileTypeHelper makeTypeHelper(ChunkType chunkType, Class<?> type, double percentile,
            boolean averageEvenlyDivided, WritableColumnSource resultColumn) {
        if (averageEvenlyDivided) {
            switch (chunkType) {
                // for things that are not int, long, double, or float we do not actually average the median;
                // we just do the standard 50-%tile thing. It might be worth defining this to be friendlier.
                case Char:
                    return new CharPercentileTypeHelper(percentile, resultColumn);
                case Byte:
                    return new BytePercentileTypeHelper(percentile, resultColumn);
                case Short:
                    return new ShortPercentileTypeHelper(percentile, resultColumn);
                case Object:
                    return makeObjectHelper(type, percentile, resultColumn);
                // For the int, long, float, and double types we actually average the adjacent values to compute the
                // median
                case Int:
                    return new IntPercentileTypeMedianHelper(percentile, resultColumn);
                case Long:
                    return new LongPercentileTypeMedianHelper(percentile, resultColumn);
                case Float:
                    return new FloatPercentileTypeMedianHelper(percentile, resultColumn);
                case Double:
                    return new DoublePercentileTypeMedianHelper(percentile, resultColumn);
                default:
                case Boolean:
                    throw new UnsupportedOperationException();
            }
        } else {
            switch (chunkType) {
                case Char:
                    return new CharPercentileTypeHelper(percentile, resultColumn);
                case Byte:
                    return new BytePercentileTypeHelper(percentile, resultColumn);
                case Short:
                    return new ShortPercentileTypeHelper(percentile, resultColumn);
                case Int:
                    return new IntPercentileTypeHelper(percentile, resultColumn);
                case Long:
                    return new LongPercentileTypeHelper(percentile, resultColumn);
                case Float:
                    return new FloatPercentileTypeHelper(percentile, resultColumn);
                case Double:
                    return new DoublePercentileTypeHelper(percentile, resultColumn);
                case Object:
                    return makeObjectHelper(type, percentile, resultColumn);
                default:
                case Boolean:
                    throw new UnsupportedOperationException();
            }
        }
    }

    @NotNull
    private static PercentileTypeHelper makeObjectHelper(
            Class<?> type,
            double percentile,
            WritableColumnSource resultColumn) {
        if (type == Boolean.class) {
            return new BooleanPercentileTypeHelper(percentile, resultColumn);
        } else if (type == Instant.class) {
            return new InstantPercentileTypeHelper(percentile, resultColumn);
        } else {
            return new ObjectPercentileTypeHelper(percentile, resultColumn);
        }
    }

    interface PercentileTypeHelper {
        boolean setResult(SegmentedSortedMultiSet ssmLo, SegmentedSortedMultiSet ssmHi, long destination);

        boolean setResultNull(long destination);

        int pivot(SegmentedSortedMultiSet ssmLo, Chunk<? extends Values> valueCopy, IntChunk<ChunkLengths> counts,
                int startPosition, int runLength, MutableInt leftOvers);

        int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet, Chunk<? extends Values> valueCopy,
                IntChunk<ChunkLengths> counts, int startPosition, int runLength);
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        stageRuns(stagedAdds, values, destinations, startPositions, length, stateModified);
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        stageRuns(stagedRemoves, values, destinations, startPositions, length, stateModified);
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        stageRuns(stagedRemoves, preValues, destinations, startPositions, length, stateModified);
        stageRuns(stagedAdds, postValues, destinations, startPositions, length, stateModified);
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return stage(stagedAdds, (int) destination, values, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return stage(stagedRemoves, (int) destination, values, 0, values.size());
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        final boolean removed = stage(stagedRemoves, (int) destination, preValues, 0, preValues.size());
        final boolean added = stage(stagedAdds, (int) destination, postValues, 0, postValues.size());
        return removed || added;
    }

    private void stageRuns(final ObjectArraySource<WritableChunk<Values>> staged, final Chunk<? extends Values> values,
            final IntChunk<RowKeys> destinations, final IntChunk<ChunkPositions> startPositions,
            final IntChunk<ChunkLengths> length, final WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            stateModified.set(ii,
                    stage(staged, destinations.get(startPosition), values, startPosition, length.get(ii)));
        }
    }

    /**
     * Append {@code values[start, start + length)} to the staged chunk for {@code destination}.
     *
     * @return true if any values were staged
     */
    private boolean stage(final ObjectArraySource<WritableChunk<Values>> staged, final int destination,
            final Chunk<? extends Values> values, final int start, final int length) {
        if (length == 0) {
            return false;
        }
        WritableChunk<Values> chunk = staged.getUnsafe(destination);
        if (chunk == null) {
            if (stagedAdds.getUnsafe(destination) == null && stagedRemoves.getUnsafe(destination) == null) {
                touchedDestinations.getAndSetUnsafe(touchedCount++, destination);
            }
            chunk = chunkType.makeWritableChunk(length);
            chunk.setSize(0);
            staged.getAndSetUnsafe(destination, chunk);
        } else if (chunk.capacity() - chunk.size() < length) {
            final long required = (long) chunk.size() + length;
            if (required > ArrayUtil.MAX_ARRAY_SIZE) {
                // Staged removals are values present before this cycle, so a destination may be applied early; it
                // remains in touchedDestinations for the values staged after it
                applyStaged(destination);
                chunk = chunkType.makeWritableChunk(length);
                chunk.setSize(0);
                staged.getAndSetUnsafe(destination, chunk);
            } else {
                final WritableChunk<Values> grown = chunkType.makeWritableChunk(
                        (int) Math.min(ArrayUtil.MAX_ARRAY_SIZE, Math.max(required, 2L * chunk.capacity())));
                grown.copyFromChunk(chunk, 0, 0, chunk.size());
                grown.setSize(chunk.size());
                chunk.close();
                chunk = grown;
                staged.getAndSetUnsafe(destination, chunk);
            }
        }
        final int size = chunk.size();
        chunk.setSize(size + length);
        chunk.copyFromChunk(values, start, size, length);
        return true;
    }

    @Override
    public void propagateInitialState(@NotNull final QueryTable resultTable, int startingDestinationsCount) {
        applyAllStaged();
    }

    @Override
    public void propagateUpdates(@NotNull final TableUpdate downstream, @NotNull final RowSet newDestinations) {
        applyAllStaged();
    }

    @Override
    public void propagateFailure(@NotNull final Throwable originalException,
            @NotNull final TableListener.Entry sourceEntry) {
        for (int ti = 0; ti < touchedCount; ++ti) {
            final int destination = touchedDestinations.getUnsafe(ti);
            closeStaged(stagedAdds, destination);
            closeStaged(stagedRemoves, destination);
        }
        touchedCount = 0;
    }

    private static void closeStaged(final ObjectArraySource<WritableChunk<Values>> staged, final int destination) {
        final WritableChunk<Values> chunk = staged.getAndSetUnsafe(destination, null);
        if (chunk != null) {
            chunk.close();
        }
    }

    private void applyAllStaged() {
        if (touchedCount == 0) {
            return;
        }
        try (final ApplyContext context = new ApplyContext(chunkType)) {
            for (int ti = 0; ti < touchedCount; ++ti) {
                applyStaged(context, touchedDestinations.getUnsafe(ti));
            }
        }
        touchedCount = 0;
    }

    /**
     * Apply the staged values of one destination outside of {@link #applyAllStaged()}; the destination stays in
     * {@link #touchedDestinations}, and is applied again with any values staged later.
     */
    private void applyStaged(final int destination) {
        try (final ApplyContext context = new ApplyContext(chunkType)) {
            applyStaged(context, destination);
        }
    }

    private void applyStaged(final ApplyContext context, final int destination) {
        final WritableChunk<Values> removes = stagedRemoves.getAndSetUnsafe(destination, null);
        final WritableChunk<Values> adds = stagedAdds.getAndSetUnsafe(destination, null);
        if (removes == null && adds == null) {
            return;
        }

        final SegmentedSortedMultiSet ssmLo = ssmLoForSlot(destination);
        final SegmentedSortedMultiSet ssmHi = ssmHiForSlot(destination);
        try {
            // Count NaN, but not NULL values
            final WritableIntChunk<ChunkLengths> removeCounts =
                    removes == null ? null : context.removeCounts(removes.size());
            if (removes != null) {
                compactAndCountKernel.compactAndCount(removes, removeCounts, false, true);
            }
            final WritableIntChunk<ChunkLengths> addCounts = adds == null ? null : context.addCounts(adds.size());
            if (adds != null) {
                compactAndCountKernel.compactAndCount(adds, addCounts, false, true);
            }
            if (removes != null && adds != null) {
                netChangeKernel.net(removes, removeCounts, adds, addCounts);
            }

            if (removes != null && removes.size() > 0) {
                pivotedRemoval(context, context.removeContext, 0, removes.size(), ssmLo, ssmHi, removes,
                        removeCounts);
            }
            if (adds != null && adds.size() > 0) {
                pivotedInsertion(context, ssmLo, ssmHi, 0, adds.size(), adds, addCounts);
            }
        } finally {
            SafeCloseable.closeAll(removes, adds);
        }

        if (ssmLo.size() == 0 && ssmHi.size() == 0) {
            clearSsm(destination, 0);
            clearSsm(destination, 1);
            percentileTypeHelper.setResultNull(destination);
            return;
        }
        percentileTypeHelper.setResult(ssmLo, ssmHi, destination);
        if (ssmLo.size() == 0) {
            clearSsm(destination, 0);
        }
        if (ssmHi.size() == 0) {
            clearSsm(destination, 1);
        }
    }

    private void pivotedRemoval(ApplyContext context, SegmentedSortedMultiSet.RemoveContext removeContext,
            int startPosition, int runLength, SegmentedSortedMultiSet ssmLo, SegmentedSortedMultiSet ssmHi,
            WritableChunk<? extends Values> valueCopy, WritableIntChunk<ChunkLengths> counts) {
        // We have no choice but to split this chunk, and furthermore to make sure that we do not remove more
        // of the maximum lo value than actually exist within ssmLo.
        final MutableInt leftOvers = new MutableInt();
        int loPivot;
        if (ssmLo.size() > 0) {
            loPivot = percentileTypeHelper.pivot(ssmLo, valueCopy, counts, startPosition, runLength, leftOvers);
            Assert.leq(leftOvers.get(), "leftOvers.get()", ssmHi.totalSize(), "ssmHi.totalSize()");
        } else {
            loPivot = 0;
        }

        if (loPivot > 0) {
            final WritableChunk<? extends Values> loValueSlice =
                    context.valueResettable.resetFromChunk(valueCopy, startPosition, loPivot);
            final WritableIntChunk<ChunkLengths> loCountSlice =
                    context.countResettable.resetFromChunk(counts, startPosition, loPivot);
            if (leftOvers.get() > 0) {
                counts.set(startPosition + loPivot - 1, counts.get(startPosition + loPivot - 1) - leftOvers.get());
            }
            ssmLo.remove(removeContext, loValueSlice, loCountSlice);
        }

        if (leftOvers.get() > 0) {
            counts.set(startPosition + loPivot - 1, leftOvers.get());
            loPivot--;
        }

        if (loPivot < runLength) {
            final WritableChunk<? extends Values> hiValueSlice =
                    context.valueResettable.resetFromChunk(valueCopy, startPosition + loPivot, runLength - loPivot);
            final WritableIntChunk<ChunkLengths> hiCountSlice =
                    context.countResettable.resetFromChunk(counts, startPosition + loPivot, runLength - loPivot);
            ssmHi.remove(removeContext, hiValueSlice, hiCountSlice);
        }
    }

    private void pivotedInsertion(ApplyContext context, SegmentedSortedMultiSet ssmLo,
            SegmentedSortedMultiSet ssmHi, int startPosition, int runLength, WritableChunk<? extends Values> valueCopy,
            WritableIntChunk<ChunkLengths> counts) {
        final int loPivot;
        if (ssmLo.size() > 0) {
            loPivot = percentileTypeHelper.pivot(ssmLo, valueCopy, counts, startPosition, runLength);
        } else {
            loPivot = 0;
        }

        if (loPivot > 0) {
            final WritableChunk<? extends Values> loValueSlice =
                    context.valueResettable.resetFromChunk(valueCopy, startPosition, loPivot);
            final WritableIntChunk<ChunkLengths> loCountSlice =
                    context.countResettable.resetFromChunk(counts, startPosition, loPivot);
            ssmLo.insert(loValueSlice, loCountSlice);
        }

        if (loPivot < runLength) {
            final WritableChunk<? extends Values> hiValueSlice =
                    context.valueResettable.resetFromChunk(valueCopy, startPosition + loPivot, runLength - loPivot);
            final WritableIntChunk<ChunkLengths> hiCountSlice =
                    context.countResettable.resetFromChunk(counts, startPosition + loPivot, runLength - loPivot);
            ssmHi.insert(hiValueSlice, hiCountSlice);
        }
    }

    private SegmentedSortedMultiSet ssmLoForSlot(long destination) {
        return ssmForSlot(destination, 0);
    }

    private SegmentedSortedMultiSet ssmHiForSlot(long destination) {
        return ssmForSlot(destination, 1);
    }

    private SegmentedSortedMultiSet ssmForSlot(long destination, int hi) {
        final long slot = destination * 2 + hi;
        SegmentedSortedMultiSet ssm = ssms.getUnsafe(slot);
        if (ssm == null) {
            ssms.set(slot, ssm = ssmFactory.get());
        }
        return ssm;
    }

    private void clearSsm(long destination, int hi) {
        final long slot = destination * 2 + hi;
        ssms.set(slot, null);
    }

    @Override
    public void ensureCapacity(long tableSize) {
        internalResult.ensureCapacity(tableSize);
        ssms.ensureCapacity(tableSize * 2);
        stagedAdds.ensureCapacity(tableSize);
        stagedRemoves.ensureCapacity(tableSize);
        touchedDestinations.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.<String, ColumnSource<?>>singletonMap(name, externalResult);
    }

    @Override
    public void startTrackingPrevValues() {
        internalResult.startTrackingPrevValues();
    }

    /**
     * Scratch state for applying staged values, shared by every destination applied together.
     */
    private static class ApplyContext implements SafeCloseable {
        final SegmentedSortedMultiSet.RemoveContext removeContext =
                SegmentedSortedMultiSet.makeRemoveContext(NODE_SIZE);
        final ResettableWritableChunk<Values> valueResettable;
        final ResettableWritableIntChunk<ChunkLengths> countResettable;
        private WritableIntChunk<ChunkLengths> removeCounts;
        private WritableIntChunk<ChunkLengths> addCounts;

        private ApplyContext(ChunkType chunkType) {
            valueResettable = chunkType.makeResettableWritableChunk();
            countResettable = ResettableWritableIntChunk.makeResettableChunk();
        }

        WritableIntChunk<ChunkLengths> removeCounts(final int size) {
            return removeCounts = sized(removeCounts, size);
        }

        WritableIntChunk<ChunkLengths> addCounts(final int size) {
            return addCounts = sized(addCounts, size);
        }

        private static WritableIntChunk<ChunkLengths> sized(final WritableIntChunk<ChunkLengths> counts,
                final int size) {
            if (counts != null && counts.capacity() >= size) {
                counts.setSize(size);
                return counts;
            }
            if (counts != null) {
                counts.close();
            }
            return WritableIntChunk.makeWritableChunk(size);
        }

        @Override
        public void close() {
            valueResettable.close();
            countResettable.close();
            if (removeCounts != null) {
                removeCounts.close();
            }
            if (addCounts != null) {
                addCounts.close();
            }
        }
    }
}
