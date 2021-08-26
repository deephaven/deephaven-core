/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by.ssmpercentile;

import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.by.IterativeChunkedAggregationOperator;
import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import io.deephaven.db.v2.ssms.SegmentedSortedMultiSet;
import io.deephaven.db.v2.utils.compact.CompactKernel;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Iterative average operator.
 */
public class SsmChunkedPercentileOperator implements IterativeChunkedAggregationOperator {
    private static final int NODE_SIZE = Configuration.getInstance()
        .getIntegerWithDefault("SsmChunkedMinMaxOperator.nodeSize", 4096);
    private final ArrayBackedColumnSource internalResult;
    private final ColumnSource externalResult;
    /**
     * Even slots hold the low values, odd slots hold the high values.
     */
    private final ObjectArraySource<SegmentedSortedMultiSet> ssms;
    private final String name;
    private final CompactKernel compactAndCountKernel;
    private final Supplier<SegmentedSortedMultiSet> ssmFactory;
    private final Supplier<SegmentedSortedMultiSet.RemoveContext> removeContextFactory;
    private final ChunkType chunkType;
    private final PercentileTypeHelper percentileTypeHelper;

    public SsmChunkedPercentileOperator(Class<?> type, double percentile, boolean averageMedian,
        String name) {
        this.name = name;
        this.ssms = new ObjectArraySource<>(SegmentedSortedMultiSet.class);
        final boolean isDateTime = type == DBDateTime.class;
        if (isDateTime) {
            chunkType = ChunkType.Long;
        } else {
            chunkType = ChunkType.fromElementType(type);
        }
        if (isDateTime) {
            internalResult = new LongArraySource();
            // noinspection unchecked
            externalResult = new BoxedColumnSource.OfDateTime(internalResult);
            averageMedian = false;
        } else {
            if (averageMedian) {
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
                        // for things that are not int, long, double, or float we do not actually
                        // average the median;
                        // we just do the standard 50-%tile thing. It might be worth defining this
                        // to be friendlier.
                        internalResult = ArrayBackedColumnSource.getMemoryColumnSource(0, type);
                }
            } else {
                internalResult = ArrayBackedColumnSource.getMemoryColumnSource(0, type);
            }
            externalResult = internalResult;
        }
        compactAndCountKernel = CompactKernel.makeCompact(chunkType);
        ssmFactory = SegmentedSortedMultiSet.makeFactory(chunkType, NODE_SIZE, type);
        removeContextFactory = SegmentedSortedMultiSet.makeRemoveContextFactory(NODE_SIZE);
        percentileTypeHelper =
            makeTypeHelper(chunkType, type, percentile, averageMedian, internalResult);
    }

    private static PercentileTypeHelper makeTypeHelper(ChunkType chunkType, Class<?> type,
        double percentile, boolean averageMedian, ArrayBackedColumnSource resultColumn) {
        if (averageMedian) {
            switch (chunkType) {
                // for things that are not int, long, double, or float we do not actually average
                // the median;
                // we just do the standard 50-%tile thing. It might be worth defining this to be
                // friendlier.
                case Char:
                    return new CharPercentileTypeHelper(percentile, resultColumn);
                case Byte:
                    return new BytePercentileTypeHelper(percentile, resultColumn);
                case Short:
                    return new ShortPercentileTypeHelper(percentile, resultColumn);
                case Object:
                    return makeObjectHelper(type, percentile, resultColumn);
                // For the int, long, float, and double types we actually average the adjacent
                // values to compute the median
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
    private static PercentileTypeHelper makeObjectHelper(Class<?> type, double percentile,
        ArrayBackedColumnSource resultColumn) {
        if (type == Boolean.class) {
            return new BooleanPercentileTypeHelper(percentile, resultColumn);
        } else if (type == DBDateTime.class) {
            return new DateTimePercentileTypeHelper(percentile, resultColumn);
        } else {
            return new ObjectPercentileTypeHelper(percentile, resultColumn);
        }
    }

    interface PercentileTypeHelper {
        boolean setResult(SegmentedSortedMultiSet ssmLo, SegmentedSortedMultiSet ssmHi,
            long destination);

        boolean setResultNull(long destination);

        int pivot(SegmentedSortedMultiSet ssmLo, Chunk<? extends Values> valueCopy,
            IntChunk<ChunkLengths> counts, int startPosition, int runLength, MutableInt leftOvers);

        int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet,
            Chunk<? extends Values> valueCopy, IntChunk<ChunkLengths> counts, int startPosition,
            int runLength);
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
        LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
        IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
        WritableBooleanChunk<Values> stateModified) {
        final BucketSsmMinMaxContext context = (BucketSsmMinMaxContext) bucketedContext;

        context.valueCopy.setSize(values.size());
        // noinspection unchecked
        context.valueCopy.copyFromChunk((Chunk) values, 0, 0, values.size());

        context.lengthCopy.setSize(length.size());
        context.lengthCopy.copyFromChunk(length, 0, 0, length.size());

        compactAndCountKernel.compactAndCount(context.valueCopy, context.counts, startPositions,
            context.lengthCopy);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final SegmentedSortedMultiSet ssmLo = ssmLoForSlot(destination);
            final SegmentedSortedMultiSet ssmHi = ssmHiForSlot(destination);

            pivotedInsertion(context, ssmLo, ssmHi, startPosition, runLength, context.valueCopy,
                context.counts);

            stateModified.set(ii, percentileTypeHelper.setResult(ssmLo, ssmHi, destination));
        }
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
        LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
        IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
        WritableBooleanChunk<Values> stateModified) {
        final BucketSsmMinMaxContext context = (BucketSsmMinMaxContext) bucketedContext;

        context.valueCopy.setSize(values.size());
        // noinspection unchecked
        context.valueCopy.copyFromChunk((Chunk) values, 0, 0, values.size());

        context.lengthCopy.setSize(length.size());
        context.lengthCopy.copyFromChunk(length, 0, 0, length.size());

        compactAndCountKernel.compactAndCount(context.valueCopy, context.counts, startPositions,
            context.lengthCopy);

        final SegmentedSortedMultiSet.RemoveContext removeContext = removeContextFactory.get();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final SegmentedSortedMultiSet ssmLo = ssmLoForSlot(destination);
            final SegmentedSortedMultiSet ssmHi = ssmHiForSlot(destination);
            pivotedRemoval(context, removeContext, startPosition, runLength, ssmLo, ssmHi,
                context.valueCopy, context.counts);

            final boolean modified = percentileTypeHelper.setResult(ssmLo, ssmHi, destination);
            if (ssmLo.size() == 0) {
                clearSsm(destination, 0);
            }
            if (ssmHi.size() == 0) {
                clearSsm(destination, 1);
            }
            stateModified.set(ii, modified);
        }
    }

    private void pivotedRemoval(SsmMinMaxContext context,
        SegmentedSortedMultiSet.RemoveContext removeContext, int startPosition, int runLength,
        SegmentedSortedMultiSet ssmLo, SegmentedSortedMultiSet ssmHi,
        WritableChunk<? extends Values> valueCopy, WritableIntChunk<ChunkLengths> counts) {
        // We have no choice but to split this chunk, and furthermore to make sure that we do not
        // remove more
        // of the maximum lo value than actually exist within ssmLo.
        final MutableInt leftOvers = new MutableInt();
        int loPivot;
        if (ssmLo.size() > 0) {
            loPivot = percentileTypeHelper.pivot(ssmLo, valueCopy, counts, startPosition, runLength,
                leftOvers);
            Assert.leq(leftOvers.intValue(), "leftOvers.intValue()", ssmHi.totalSize(),
                "ssmHi.totalSize()");
        } else {
            loPivot = 0;
        }

        if (loPivot > 0) {
            final WritableChunk<? extends Values> loValueSlice =
                context.valueResettable.resetFromChunk(valueCopy, startPosition, loPivot);
            final WritableIntChunk<ChunkLengths> loCountSlice =
                context.countResettable.resetFromChunk(counts, startPosition, loPivot);
            if (leftOvers.intValue() > 0) {
                counts.set(startPosition + loPivot - 1,
                    counts.get(startPosition + loPivot - 1) - leftOvers.intValue());
            }
            ssmLo.remove(removeContext, loValueSlice, loCountSlice);
        }

        if (leftOvers.intValue() > 0) {
            counts.set(startPosition + loPivot - 1, leftOvers.intValue());
            loPivot--;
        }

        if (loPivot < runLength) {
            final WritableChunk<? extends Values> hiValueSlice = context.valueResettable
                .resetFromChunk(valueCopy, startPosition + loPivot, runLength - loPivot);
            final WritableIntChunk<ChunkLengths> hiCountSlice = context.countResettable
                .resetFromChunk(counts, startPosition + loPivot, runLength - loPivot);
            ssmHi.remove(removeContext, hiValueSlice, hiCountSlice);
        }
    }

    private void pivotedInsertion(SsmMinMaxContext context, SegmentedSortedMultiSet ssmLo,
        SegmentedSortedMultiSet ssmHi, int startPosition, int runLength,
        WritableChunk<? extends Values> valueCopy, WritableIntChunk<ChunkLengths> counts) {
        final int loPivot;
        if (ssmLo.size() > 0) {
            loPivot =
                percentileTypeHelper.pivot(ssmLo, valueCopy, counts, startPosition, runLength);
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
            final WritableChunk<? extends Values> hiValueSlice = context.valueResettable
                .resetFromChunk(valueCopy, startPosition + loPivot, runLength - loPivot);
            final WritableIntChunk<ChunkLengths> hiCountSlice = context.countResettable
                .resetFromChunk(counts, startPosition + loPivot, runLength - loPivot);
            ssmHi.insert(hiValueSlice, hiCountSlice);
        }
    }


    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> preValues,
        Chunk<? extends Values> postValues, LongChunk<? extends KeyIndices> postShiftIndices,
        IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions,
        IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final BucketSsmMinMaxContext context = (BucketSsmMinMaxContext) bucketedContext;

        context.valueCopy.setSize(preValues.size());
        // noinspection unchecked
        context.valueCopy.copyFromChunk((Chunk) preValues, 0, 0, preValues.size());

        context.lengthCopy.setSize(length.size());
        context.lengthCopy.copyFromChunk(length, 0, 0, length.size());

        compactAndCountKernel.compactAndCount(context.valueCopy, context.counts, startPositions,
            context.lengthCopy);

        final SegmentedSortedMultiSet.RemoveContext removeContext = removeContextFactory.get();
        context.ssmsToMaybeClear.fillWithValue(0, destinations.size(), false);
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final SegmentedSortedMultiSet ssmLo = ssmLoForSlot(destination);
            final SegmentedSortedMultiSet ssmHi = ssmHiForSlot(destination);

            pivotedRemoval(context, removeContext, startPosition, runLength, ssmLo, ssmHi,
                context.valueCopy, context.counts);
            if (ssmLo.size() == 0 && ssmHi.size() == 0) {
                context.ssmsToMaybeClear.set(ii, true);
            }
        }

        context.valueCopy.setSize(postValues.size());
        // noinspection unchecked
        context.valueCopy.copyFromChunk((Chunk) postValues, 0, 0, postValues.size());

        context.lengthCopy.setSize(length.size());
        context.lengthCopy.copyFromChunk(length, 0, 0, length.size());

        compactAndCountKernel.compactAndCount(context.valueCopy, context.counts, startPositions,
            context.lengthCopy);
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            if (runLength == 0) {
                if (context.ssmsToMaybeClear.get(ii)) {
                    // we may have deleted this position on the last round, really get rid of it
                    clearSsm(destination, 0);
                    clearSsm(destination, 1);
                    stateModified.set(ii, percentileTypeHelper.setResultNull(destination));
                } else {
                    stateModified.set(ii, percentileTypeHelper.setResult(ssmLoForSlot(destination),
                        ssmHiForSlot(destination), destination));
                }
                continue;
            }

            final SegmentedSortedMultiSet ssmLo = ssmLoForSlot(destination);
            final SegmentedSortedMultiSet ssmHi = ssmHiForSlot(destination);

            pivotedInsertion(context, ssmLo, ssmHi, startPosition, runLength, context.valueCopy,
                context.counts);

            stateModified.set(ii, percentileTypeHelper.setResult(ssmLo, ssmHi, destination));
        }
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize,
        Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices,
        long destination) {
        final SsmMinMaxContext context = (SsmMinMaxContext) singletonContext;

        context.valueCopy.setSize(values.size());
        // noinspection unchecked
        context.valueCopy.copyFromChunk((Chunk) values, 0, 0, values.size());
        compactAndCountKernel.compactAndCount(context.valueCopy, context.counts);
        final SegmentedSortedMultiSet ssmLo = ssmLoForSlot(destination);
        final SegmentedSortedMultiSet ssmHi = ssmHiForSlot(destination);
        if (context.valueCopy.size() > 0) {
            pivotedInsertion(context, ssmLo, ssmHi, 0, context.valueCopy.size(), context.valueCopy,
                context.counts);
        }
        return percentileTypeHelper.setResult(ssmLo, ssmHi, destination);
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize,
        Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices,
        long destination) {
        final SsmMinMaxContext context = (SsmMinMaxContext) singletonContext;

        context.valueCopy.setSize(values.size());
        // noinspection unchecked
        context.valueCopy.copyFromChunk((Chunk) values, 0, 0, values.size());
        compactAndCountKernel.compactAndCount(context.valueCopy, context.counts);
        if (context.valueCopy.size() == 0) {
            return false;
        }
        final SegmentedSortedMultiSet ssmLo = ssmLoForSlot(destination);
        final SegmentedSortedMultiSet ssmHi = ssmHiForSlot(destination);

        pivotedRemoval(context, context.removeContext, 0, context.valueCopy.size(), ssmLo, ssmHi,
            context.valueCopy, context.counts);
        final boolean modified = percentileTypeHelper.setResult(ssmLo, ssmHi, destination);
        if (ssmLo.size() == 0) {
            clearSsm(destination, 0);
        }
        if (ssmHi.size() == 0) {
            clearSsm(destination, 1);
        }
        return modified;
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize,
        Chunk<? extends Values> preValues, Chunk<? extends Values> postValues,
        LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
        final SsmMinMaxContext context = (SsmMinMaxContext) singletonContext;

        context.valueCopy.setSize(preValues.size());
        // noinspection unchecked
        context.valueCopy.copyFromChunk((Chunk) preValues, 0, 0, preValues.size());
        compactAndCountKernel.compactAndCount(context.valueCopy, context.counts);
        SegmentedSortedMultiSet ssmLo = null;
        SegmentedSortedMultiSet ssmHi = null;
        if (context.valueCopy.size() > 0) {
            ssmLo = ssmLoForSlot(destination);
            ssmHi = ssmHiForSlot(destination);

            pivotedRemoval(context, context.removeContext, 0, context.valueCopy.size(), ssmLo,
                ssmHi, context.valueCopy, context.counts);
        }

        context.valueCopy.setSize(postValues.size());
        // noinspection unchecked
        context.valueCopy.copyFromChunk((Chunk) postValues, 0, 0, postValues.size());
        compactAndCountKernel.compactAndCount(context.valueCopy, context.counts);
        if (context.valueCopy.size() > 0) {
            if (ssmLo == null) {
                ssmLo = ssmLoForSlot(destination);
                ssmHi = ssmHiForSlot(destination);
            }
            pivotedInsertion(context, ssmLo, ssmHi, 0, context.valueCopy.size(), context.valueCopy,
                context.counts);
            return percentileTypeHelper.setResult(ssmLo, ssmHi, destination);
        } else if (ssmLo != null && ssmLo.size() == 0 && ssmHi.size() == 0) {
            clearSsm(destination, 0);
            clearSsm(destination, 1);
            return percentileTypeHelper.setResultNull(destination);
        } else if (ssmLo == null) {
            return false;
        } else {
            return percentileTypeHelper.setResult(ssmLo, ssmHi, destination);
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
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.<String, ColumnSource<?>>singletonMap(name, externalResult);
    }

    @Override
    public void startTrackingPrevValues() {
        internalResult.startTrackingPrevValues();
    }

    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new BucketSsmMinMaxContext(chunkType, size);
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new SsmMinMaxContext(chunkType, size);
    }

    private static class SsmMinMaxContext implements SingletonContext {
        final SegmentedSortedMultiSet.RemoveContext removeContext =
            SegmentedSortedMultiSet.makeRemoveContext(NODE_SIZE);
        final WritableChunk<? extends Values> valueCopy;
        final WritableIntChunk<ChunkLengths> counts;
        final ResettableWritableChunk<Values> valueResettable;
        final ResettableWritableIntChunk<ChunkLengths> countResettable;

        private SsmMinMaxContext(ChunkType chunkType, int size) {
            valueCopy = chunkType.makeWritableChunk(size);
            counts = WritableIntChunk.makeWritableChunk(size);
            valueResettable = chunkType.makeResettableWritableChunk();
            countResettable = ResettableWritableIntChunk.makeResettableChunk();
        }

        @Override
        public void close() {
            valueCopy.close();
            counts.close();
            valueResettable.close();
            countResettable.close();
        }
    }

    private static class BucketSsmMinMaxContext extends SsmMinMaxContext
        implements BucketedContext {
        final WritableIntChunk<ChunkLengths> lengthCopy;
        final WritableBooleanChunk ssmsToMaybeClear;

        private BucketSsmMinMaxContext(ChunkType chunkType, int size) {
            super(chunkType, size);
            lengthCopy = WritableIntChunk.makeWritableChunk(size);
            ssmsToMaybeClear = WritableBooleanChunk.makeWritableChunk(size);
        }

        @Override
        public void close() {
            super.close();
            lengthCopy.close();
            ssmsToMaybeClear.close();
        }
    }
}
