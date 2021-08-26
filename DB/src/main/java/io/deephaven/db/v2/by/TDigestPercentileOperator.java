/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkLengths;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkPositions;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
import io.deephaven.db.v2.utils.cast.ToDoubleCast;
import com.tdunning.math.stats.TDigest;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Iterative average operator.
 */
public class TDigestPercentileOperator implements IterativeChunkedAggregationOperator {
    private final DoubleArraySource[] resultColumns;
    private final double[] percentiles;
    private final ObjectArraySource<TDigest> digests;
    private final String[] resultNames;
    private final Supplier<TDigest> digestFactory;
    private final ChunkType chunkType;
    private final String digestColumnName;

    public TDigestPercentileOperator(@NotNull Class<?> type, double compression, double percentile,
        @NotNull String name) {
        this(type, compression, null, new double[] {percentile}, new String[] {name});
    }

    public TDigestPercentileOperator(@NotNull Class<?> type, double compression,
        String digestColumnName, @NotNull double[] percentiles, @NotNull String[] resultNames) {
        if (resultNames.length != percentiles.length) {
            throw new IllegalArgumentException(
                "Percentile length and resultName length must be identical:" + resultNames.length
                    + " (resultNames) != " + percentiles.length + " (percentiles)");
        }
        this.percentiles = percentiles;
        this.digestColumnName = digestColumnName;
        this.resultNames = resultNames;
        this.digests = new ObjectArraySource<>(TDigest.class);
        final boolean isDateTime = type == DBDateTime.class;
        if (isDateTime) {
            throw new UnsupportedOperationException(
                "DateTime is not supported for approximate percentiles.");
        }
        chunkType = ChunkType.fromElementType(type);
        resultColumns = new DoubleArraySource[percentiles.length];
        for (int ii = 0; ii < percentiles.length; ++ii) {
            resultColumns[ii] = new DoubleArraySource();
        }
        digestFactory = () -> TDigest.createDigest(compression);
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
        LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
        IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
        WritableBooleanChunk<Values> stateModified) {
        final TDigestContext tDigestContext = (TDigestContext) bucketedContext;
        final DoubleChunk<? extends Values> doubleValues = tDigestContext.toDoubleCast.cast(values);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = length.get(ii);
            if (runLength == 0) {
                continue;
            }
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final TDigest digest = digestForSlot(destination);

            int added = 0;
            for (int jj = startPosition; jj < startPosition + runLength; ++jj) {
                final double x = doubleValues.get(jj);
                if (Double.isNaN(x) || x == QueryConstants.NULL_DOUBLE) {
                    continue;
                }
                added++;
                digest.add(x);
            }
            if (added > 0) {
                stateModified.set(ii, true);
            }
        }
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
        LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
        IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
        WritableBooleanChunk<Values> stateModified) {
        throw new UnsupportedOperationException(
            "t-digest Approximate percentiles do not support data removal.");
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> preValues,
        Chunk<? extends Values> postValues, LongChunk<? extends KeyIndices> postShiftIndices,
        IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions,
        IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        throw new UnsupportedOperationException(
            "t-digest Approximate percentiles do not support data modification.");
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize,
        Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices,
        long destination) {
        final TDigestContext tDigestContext = (TDigestContext) singletonContext;
        final TDigest digest = digestForSlot(destination);
        final DoubleChunk<? extends Values> doubleValues = tDigestContext.toDoubleCast.cast(values);

        int added = 0;
        for (int ii = 0; ii < doubleValues.size(); ++ii) {
            final double x = doubleValues.get(ii);
            if (Double.isNaN(x) || x == QueryConstants.NULL_DOUBLE) {
                continue;
            }
            added++;
            digest.add(x);
        }

        return added > 0;
    }

    @Override
    public void propagateInitialState(@NotNull QueryTable resultTable) {
        resultTable.getIndex().forAllLongs(this::updateDestination);
    }

    private void updateDestination(long destination) {
        final TDigest digest = digestForSlot(destination);
        for (int jj = 0; jj < resultColumns.length; ++jj) {
            resultColumns[jj].set(destination, digest.quantile(percentiles[jj]));
        }
    }

    @Override
    public void propagateUpdates(@NotNull ShiftAwareListener.Update downstream,
        @NotNull ReadOnlyIndex newDestinations) {
        downstream.added.forAllLongs(this::updateDestination);
        downstream.modified.forAllLongs(this::updateDestination);
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize,
        Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices,
        long destination) {
        throw new UnsupportedOperationException(
            "t-digest Approximate percentiles do not support data removal.");
    }

    private TDigest digestForSlot(long slot) {
        TDigest digest = digests.getUnsafe(slot);
        if (digest == null) {
            digests.set(slot, digest = digestFactory.get());
        }
        return digest;
    }

    @Override
    public void ensureCapacity(long tableSize) {
        for (int jj = 0; jj < resultColumns.length; ++jj) {
            resultColumns[jj].ensureCapacity(tableSize);
        }
        digests.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        final Map<String, ColumnSource<?>> results = new LinkedHashMap<>(resultNames.length);
        if (digestColumnName != null) {
            results.put(digestColumnName, digests);
        }
        for (int jj = 0; jj < resultColumns.length; ++jj) {
            results.put(resultNames[jj], resultColumns[jj]);
        }
        return results;
    }

    @Override
    public void startTrackingPrevValues() {
        for (final DoubleArraySource resultColumn : resultColumns) {
            resultColumn.startTrackingPrevValues();
        }
    }

    private class TDigestContext implements SingletonContext, BucketedContext {
        final ToDoubleCast toDoubleCast;

        private TDigestContext(int size) {
            toDoubleCast = ToDoubleCast.makeToDoubleCast(chunkType, size);
        }

        @Override
        public void close() {
            toDoubleCast.close();
        }
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new TDigestContext(size);
    }

    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new TDigestContext(size);
    }
}
