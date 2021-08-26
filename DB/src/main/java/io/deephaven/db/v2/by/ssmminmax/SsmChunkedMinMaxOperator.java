/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by.ssmminmax;

import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.by.IterativeChunkedAggregationOperator;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ObjectArraySource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import io.deephaven.db.v2.ssms.SegmentedSortedMultiSet;
import io.deephaven.db.v2.utils.compact.CompactKernel;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Iterative average operator.
 */
public class SsmChunkedMinMaxOperator implements IterativeChunkedAggregationOperator {
    private static final int NODE_SIZE =
            Configuration.getInstance().getIntegerWithDefault("SsmChunkedMinMaxOperator.nodeSize", 4096);
    private final ArrayBackedColumnSource resultColumn;
    private final ObjectArraySource<SegmentedSortedMultiSet> ssms;
    private final String name;
    private final CompactKernel compactAndCountKernel;
    private final Supplier<SegmentedSortedMultiSet> ssmFactory;
    private final Supplier<SegmentedSortedMultiSet.RemoveContext> removeContextFactory;
    private final ChunkType chunkType;
    private final SetResult setResult;

    public SsmChunkedMinMaxOperator(
            // region extra constructor params
            Class<?> type,
            // endregion extra constructor params
            boolean minimum, String name) {
        this.name = name;
        this.ssms = new ObjectArraySource<>(SegmentedSortedMultiSet.class);
        // region resultColumn initialization
        this.resultColumn = ArrayBackedColumnSource.getMemoryColumnSource(0, type);
        // endregion resultColumn initialization
        if (type == DBDateTime.class) {
            chunkType = ChunkType.Long;
        } else {
            chunkType = ChunkType.fromElementType(type);
        }
        compactAndCountKernel = CompactKernel.makeCompact(chunkType);
        ssmFactory = SegmentedSortedMultiSet.makeFactory(chunkType, NODE_SIZE, type);
        removeContextFactory = SegmentedSortedMultiSet.makeRemoveContextFactory(NODE_SIZE);
        setResult = makeSetResult(chunkType, type, minimum, resultColumn);
    }

    private static SetResult makeSetResult(ChunkType chunkType, Class<?> type, boolean minimum,
            ArrayBackedColumnSource resultColumn) {
        if (type == DBDateTime.class) {
            return new DateTimeSetResult(minimum, resultColumn);
        } else if (type == Boolean.class) {
            return new BooleanSetResult(minimum, resultColumn);
        }
        switch (chunkType) {
            case Char:
                return new CharSetResult(minimum, resultColumn);
            case Byte:
                return new ByteSetResult(minimum, resultColumn);
            case Short:
                return new ShortSetResult(minimum, resultColumn);
            case Int:
                return new IntSetResult(minimum, resultColumn);
            case Long:
                return new LongSetResult(minimum, resultColumn);
            case Float:
                return new FloatSetResult(minimum, resultColumn);
            case Double:
                return new DoubleSetResult(minimum, resultColumn);
            case Object:
                return new ObjectSetResult(minimum, resultColumn);
            default:
            case Boolean:
                throw new UnsupportedOperationException();
        }
    }

    interface SetResult {
        boolean setResult(SegmentedSortedMultiSet ssm, long destination);

        boolean setResultNull(long destination);
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final BucketSsmMinMaxContext context = (BucketSsmMinMaxContext) bucketedContext;

        context.valueCopy.setSize(values.size());
        context.valueCopy.copyFromChunk(values, 0, 0, values.size());

        context.lengthCopy.setSize(length.size());
        context.lengthCopy.copyFromChunk(length, 0, 0, length.size());

        compactAndCountKernel.compactAndCount(context.valueCopy, context.counts, startPositions, context.lengthCopy);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);


            final SegmentedSortedMultiSet ssm = ssmForSlot(destination);
            final WritableChunk<? extends Values> valueSlice =
                    context.valueResettable.resetFromChunk(context.valueCopy, startPosition, runLength);
            final WritableIntChunk<ChunkLengths> countSlice =
                    context.countResettable.resetFromChunk(context.counts, startPosition, runLength);
            ssm.insert(valueSlice, countSlice);

            stateModified.set(ii, setResult.setResult(ssm, destination));
        }
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final BucketSsmMinMaxContext context = (BucketSsmMinMaxContext) bucketedContext;

        context.valueCopy.setSize(values.size());
        context.valueCopy.copyFromChunk(values, 0, 0, values.size());

        context.lengthCopy.setSize(length.size());
        context.lengthCopy.copyFromChunk(length, 0, 0, length.size());

        compactAndCountKernel.compactAndCount(context.valueCopy, context.counts, startPositions, context.lengthCopy);

        final SegmentedSortedMultiSet.RemoveContext removeContext = removeContextFactory.get();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final SegmentedSortedMultiSet ssm = ssmForSlot(destination);
            final WritableChunk<? extends Values> valueSlice =
                    context.valueResettable.resetFromChunk(context.valueCopy, startPosition, runLength);
            final WritableIntChunk<ChunkLengths> countSlice =
                    context.countResettable.resetFromChunk(context.counts, startPosition, runLength);
            ssm.remove(removeContext, valueSlice, countSlice);
            if (ssm.size() == 0) {
                clearSsm(destination);
            }
            stateModified.set(ii, setResult.setResult(ssm, destination));
        }
    }


    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, LongChunk<? extends KeyIndices> postShiftIndices,
            IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final BucketSsmMinMaxContext context = (BucketSsmMinMaxContext) bucketedContext;

        context.valueCopy.setSize(preValues.size());
        context.valueCopy.copyFromChunk(preValues, 0, 0, preValues.size());

        context.lengthCopy.setSize(length.size());
        context.lengthCopy.copyFromChunk(length, 0, 0, length.size());

        compactAndCountKernel.compactAndCount(context.valueCopy, context.counts, startPositions, context.lengthCopy);

        final SegmentedSortedMultiSet.RemoveContext removeContext = removeContextFactory.get();
        context.ssmsToMaybeClear.fillWithValue(0, destinations.size(), false);
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final SegmentedSortedMultiSet ssm = ssmForSlot(destination);
            final WritableChunk<? extends Values> valueSlice =
                    context.valueResettable.resetFromChunk(context.valueCopy, startPosition, runLength);
            final WritableIntChunk<ChunkLengths> countSlice =
                    context.countResettable.resetFromChunk(context.counts, startPosition, runLength);
            ssm.remove(removeContext, valueSlice, countSlice);
            if (ssm.size() == 0) {
                context.ssmsToMaybeClear.set(ii, true);
            }
        }

        context.valueCopy.setSize(postValues.size());
        context.valueCopy.copyFromChunk(postValues, 0, 0, postValues.size());

        context.lengthCopy.setSize(length.size());
        context.lengthCopy.copyFromChunk(length, 0, 0, length.size());

        compactAndCountKernel.compactAndCount(context.valueCopy, context.counts, startPositions, context.lengthCopy);
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            if (runLength == 0) {
                if (context.ssmsToMaybeClear.get(ii)) {
                    // we may have deleted this position on the last round, really get rid of it
                    clearSsm(destination);
                    stateModified.set(ii, setResult.setResultNull(destination));
                } else {
                    stateModified.set(ii, setResult.setResult(ssmForSlot(destination), destination));
                }
                continue;
            }

            final SegmentedSortedMultiSet ssm = ssmForSlot(destination);
            final WritableChunk<? extends Values> valueSlice =
                    context.valueResettable.resetFromChunk(context.valueCopy, startPosition, runLength);
            final WritableIntChunk<ChunkLengths> countSlice =
                    context.countResettable.resetFromChunk(context.counts, startPosition, runLength);
            ssm.insert(valueSlice, countSlice);
            stateModified.set(ii, setResult.setResult(ssm, destination));
        }
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, long destination) {
        final SsmMinMaxContext context = (SsmMinMaxContext) singletonContext;

        context.valueCopy.setSize(values.size());
        context.valueCopy.copyFromChunk(values, 0, 0, values.size());
        compactAndCountKernel.compactAndCount(context.valueCopy, context.counts);
        final SegmentedSortedMultiSet ssm = ssmForSlot(destination);
        if (context.valueCopy.size() > 0) {
            ssm.insert(context.valueCopy, context.counts);
        }
        return setResult.setResult(ssm, destination);
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, long destination) {
        final SsmMinMaxContext context = (SsmMinMaxContext) singletonContext;

        context.valueCopy.setSize(values.size());
        context.valueCopy.copyFromChunk(values, 0, 0, values.size());
        compactAndCountKernel.compactAndCount(context.valueCopy, context.counts);
        if (context.valueCopy.size() == 0) {
            return false;
        }
        final SegmentedSortedMultiSet ssm = ssmForSlot(destination);
        ssm.remove(context.removeContext, context.valueCopy, context.counts);
        if (ssm.size() == 0) {
            clearSsm(destination);
        }
        return setResult.setResult(ssm, destination);
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
        final SsmMinMaxContext context = (SsmMinMaxContext) singletonContext;

        context.valueCopy.setSize(preValues.size());
        context.valueCopy.copyFromChunk(preValues, 0, 0, preValues.size());
        compactAndCountKernel.compactAndCount(context.valueCopy, context.counts);
        SegmentedSortedMultiSet ssm = null;
        if (context.valueCopy.size() > 0) {
            ssm = ssmForSlot(destination);
            ssm.remove(context.removeContext, context.valueCopy, context.counts);
        }

        context.valueCopy.setSize(postValues.size());
        context.valueCopy.copyFromChunk(postValues, 0, 0, postValues.size());
        compactAndCountKernel.compactAndCount(context.valueCopy, context.counts);
        if (context.valueCopy.size() > 0) {
            if (ssm == null) {
                ssm = ssmForSlot(destination);
            }
            ssm.insert(context.valueCopy, context.counts);
            return setResult.setResult(ssm, destination);
        } else if (ssm != null && ssm.size() == 0) {
            clearSsm(destination);
            return setResult.setResultNull(destination);
        } else if (ssm == null) {
            return false;
        } else {
            return setResult.setResult(ssm, destination);
        }
    }

    private SegmentedSortedMultiSet ssmForSlot(long destination) {
        SegmentedSortedMultiSet ssa = ssms.getUnsafe(destination);
        if (ssa == null) {
            ssms.set(destination, ssa = ssmFactory.get());
        }
        return ssa;
    }

    private void clearSsm(long destination) {
        ssms.set(destination, null);
    }

    @Override
    public void ensureCapacity(long tableSize) {
        resultColumn.ensureCapacity(tableSize);
        ssms.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.<String, ColumnSource<?>>singletonMap(name, resultColumn);
    }

    @Override
    public void startTrackingPrevValues() {
        resultColumn.startTrackingPrevValues();
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
        final WritableChunk<Values> valueCopy;
        final WritableIntChunk<ChunkLengths> counts;

        private SsmMinMaxContext(ChunkType chunkType, int size) {
            valueCopy = chunkType.makeWritableChunk(size);
            counts = WritableIntChunk.makeWritableChunk(size);
        }

        @Override
        public void close() {
            valueCopy.close();
            counts.close();
        }
    }

    private static class BucketSsmMinMaxContext extends SsmMinMaxContext implements BucketedContext {
        final WritableIntChunk<ChunkLengths> lengthCopy;
        final ResettableWritableChunk<Values> valueResettable;
        final ResettableWritableIntChunk<ChunkLengths> countResettable;
        final WritableBooleanChunk ssmsToMaybeClear;

        private BucketSsmMinMaxContext(ChunkType chunkType, int size) {
            super(chunkType, size);
            lengthCopy = WritableIntChunk.makeWritableChunk(size);
            valueResettable = chunkType.makeResettableWritableChunk();
            countResettable = ResettableWritableIntChunk.makeResettableChunk();
            ssmsToMaybeClear = WritableBooleanChunk.makeWritableChunk(size);
        }

        @Override
        public void close() {
            super.close();
            lengthCopy.close();
            valueResettable.close();
            countResettable.close();
            ssmsToMaybeClear.close();
        }
    }

    public IterativeChunkedAggregationOperator makeSecondaryOperator(boolean isMinimum, String resultName) {
        return new SecondaryOperator(isMinimum, resultName);
    }

    private class SecondaryOperator implements IterativeChunkedAggregationOperator {
        private final ArrayBackedColumnSource resultColumn;
        private final String resultName;
        private final SetResult setResult;

        private SecondaryOperator(boolean isMinimum, String resultName) {
            // noinspection unchecked
            this.resultColumn = ArrayBackedColumnSource.getMemoryColumnSource(0,
                    SsmChunkedMinMaxOperator.this.resultColumn.getType());
            setResult = makeSetResult(chunkType, resultColumn.getType(), isMinimum, resultColumn);
            this.resultName = resultName;
        }

        @Override
        public void addChunk(BucketedContext context, Chunk<? extends Values> values,
                LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
                IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
                WritableBooleanChunk<Values> stateModified) {
            updateBucketed(destinations, startPositions, stateModified);
        }

        @Override
        public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
                LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
                IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
                WritableBooleanChunk<Values> stateModified) {
            updateBucketed(destinations, startPositions, stateModified);
        }

        @Override
        public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices,
                IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions,
                IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
            updateBucketed(destinations, startPositions, stateModified);
        }

        private void updateBucketed(IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions,
                WritableBooleanChunk<Values> stateModified) {
            for (int ii = 0; ii < startPositions.size(); ++ii) {
                final int startPosition = startPositions.get(ii);
                final long destination = destinations.get(startPosition);
                final SegmentedSortedMultiSet ssm = ssmForSlot(destination);
                stateModified.set(ii, setResult.setResult(ssm, destination));
            }
        }

        @Override
        public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
                LongChunk<? extends KeyIndices> inputIndices, long destination) {
            return updateSingleton(destination);
        }


        @Override
        public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
                LongChunk<? extends KeyIndices> inputIndices, long destination) {
            return updateSingleton(destination);
        }

        @Override
        public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
            return updateSingleton(destination);
        }

        private boolean updateSingleton(long destination) {
            final SegmentedSortedMultiSet ssm = ssmForSlot(destination);
            return setResult.setResult(ssm, destination);
        }

        @Override
        public void ensureCapacity(long tableSize) {
            resultColumn.ensureCapacity(tableSize);
        }

        @Override
        public Map<String, ? extends ColumnSource<?>> getResultColumns() {
            return Collections.<String, ColumnSource<?>>singletonMap(resultName, resultColumn);
        }

        @Override
        public void startTrackingPrevValues() {
            resultColumn.startTrackingPrevValues();
        }
    }
}
