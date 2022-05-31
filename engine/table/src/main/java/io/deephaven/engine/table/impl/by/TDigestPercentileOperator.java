/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.by;

import com.tdunning.math.stats.TDigest;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.util.cast.ToDoubleCast;
import io.deephaven.time.DateTime;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Iterative T-Digest and percentile operator.
 */
public class TDigestPercentileOperator implements IterativeChunkedAggregationOperator {

    private final double compression;
    private final double[] percentiles;
    private final String digestColumnName;
    private final String[] resultNames;

    private final ChunkType chunkType;
    private final ObjectArraySource<TDigest> digests;
    private final DoubleArraySource[] resultColumns;
    private final Supplier<TDigest> digestFactory;

    private long firstNewDestination;
    private boolean modifiedThisStep;
    private WritableIntChunk<ChunkPositions> chunkModifiedPositions;

    public TDigestPercentileOperator(@NotNull Class<?> type, double compression, double percentile,
            @NotNull String name) {
        this(type, compression, null, new double[] {percentile}, new String[] {name});
    }

    public TDigestPercentileOperator(@NotNull Class<?> type, double compression, String digestColumnName,
            @NotNull double[] percentiles, @NotNull String[] resultNames) {
        if (resultNames.length != percentiles.length) {
            throw new IllegalArgumentException("percentiles length and resultName length must be identical:"
                    + resultNames.length + " (resultNames) != " + percentiles.length + " (percentiles)");
        }
        if (resultNames.length == 0 && digestColumnName == null) {
            throw new IllegalArgumentException(
                    "Must have at least one result column; must have at least one percentile result or provide a digest column name to expose");
        }
        this.compression = compression;
        this.percentiles = percentiles;
        this.digestColumnName = digestColumnName;
        this.resultNames = resultNames;
        this.digests = new ObjectArraySource<>(TDigest.class);
        final boolean isDateTime = type == DateTime.class;
        if (isDateTime) {
            throw new UnsupportedOperationException("DateTime is not supported for approximate percentiles.");
        }
        chunkType = ChunkType.fromElementType(type);
        resultColumns = new DoubleArraySource[percentiles.length];
        for (int ii = 0; ii < percentiles.length; ++ii) {
            resultColumns[ii] = new DoubleArraySource();
        }
        digestFactory = () -> TDigest.createDigest(compression);
    }

    public double compression() {
        return compression;
    }

    private static UnsupportedOperationException modificationUnsupported() {
        return new UnsupportedOperationException("t-digest Approximate percentiles do not support data modification");
    }

    private static UnsupportedOperationException removalUnsupported() {
        return new UnsupportedOperationException("t-digest Approximate percentiles do not support data removal");
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final TDigestContext tDigestContext = (TDigestContext) bucketedContext;
        final DoubleChunk<? extends Values> doubleValues = tDigestContext.toDoubleCast.cast(values);

        chunkModifiedPositions.setSize(0);
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
                if (destination < firstNewDestination) {
                    modifiedThisStep = true;
                }
                stateModified.set(ii, true);
                chunkModifiedPositions.add(ii);
            }
        }
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        throw removalUnsupported();
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        throw modificationUnsupported();
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final TDigestContext tDigestContext = (TDigestContext) singletonContext;
        final TDigest digest = digestForSlot(destination);
        final DoubleChunk<? extends Values> doubleValues = tDigestContext.toDoubleCast.cast(values);

        chunkModifiedPositions.setSize(0);
        int added = 0;
        for (int ii = 0; ii < doubleValues.size(); ++ii) {
            final double x = doubleValues.get(ii);
            if (Double.isNaN(x) || x == QueryConstants.NULL_DOUBLE) {
                continue;
            }
            added++;
            digest.add(x);
        }

        if (added > 0) {
            if (destination < firstNewDestination) {
                modifiedThisStep = true;
            }
            chunkModifiedPositions.add(0);
            return true;
        }
        return false;
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        throw removalUnsupported();
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        throw modificationUnsupported();
    }

    @Override
    public void propagateInitialState(@NotNull final QueryTable resultTable) {
        if (resultColumns.length == 0) {
            return;
        }
        firstNewDestination = resultTable.getRowSet().lastRowKey() + 1;
        resultTable.getRowSet().forAllRowKeys(this::updateDestination);
    }

    private void updateDestination(final long destination) {
        final TDigest digest = digestForSlot(destination);
        for (int jj = 0; jj < resultColumns.length; ++jj) {
            resultColumns[jj].set(destination, digest.quantile(percentiles[jj]));
        }
    }

    @Override
    public void resetForStep(@NotNull final TableUpdate upstream) {
        modifiedThisStep = false;
    }

    @Override
    public void propagateUpdates(@NotNull final TableUpdate downstream, @NotNull final RowSet newDestinations) {
        if (resultColumns.length == 0) {
            return;
        }
        firstNewDestination = newDestinations.lastRowKey() + 1;
        downstream.added().forAllRowKeys(this::updateDestination);
        if (modifiedThisStep) {
            downstream.modified().forAllRowKeys(this::updateDestination);
        }
    }

    private TDigest digestForSlot(final long slot) {
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
            Assert.eqNull(chunkModifiedPositions, "chunkModifiedPositions");
            chunkModifiedPositions = WritableIntChunk.makeWritableChunk(size);
        }

        @Override
        public void close() {
            toDoubleCast.close();
            chunkModifiedPositions.close();
            chunkModifiedPositions = null;
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

    public IterativeChunkedAggregationOperator makeSecondaryOperator(double percentile, @NotNull String resultName) {
        return new SecondaryOperator(percentile, resultName);
    }

    private class SecondaryOperator implements IterativeChunkedAggregationOperator {

        private final double percentile;
        private final String resultName;

        private final DoubleArraySource resultColumn;

        public SecondaryOperator(final double percentile, @NotNull final String resultName) {
            this.percentile = percentile;
            this.resultName = resultName;

            resultColumn = new DoubleArraySource();
        }


        @Override
        public void addChunk(BucketedContext context, Chunk<? extends Values> values,
                LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
                IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
                WritableBooleanChunk<Values> stateModified) {
            final int numModifiedPositions = chunkModifiedPositions.size();
            for (int mpi = 0; mpi < numModifiedPositions; ++mpi) {
                stateModified.set(chunkModifiedPositions.get(mpi), true);
            }
        }

        @Override
        public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
                LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
                IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
                WritableBooleanChunk<Values> stateModified) {
            throw removalUnsupported();
        }

        @Override
        public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys,
                IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
                WritableBooleanChunk<Values> stateModified) {
            throw modificationUnsupported();
        }

        @Override
        public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
                LongChunk<? extends RowKeys> inputRowKeys, long destination) {
            return chunkModifiedPositions.size() > 0;
        }

        @Override
        public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
                LongChunk<? extends RowKeys> inputRowKeys, long destination) {
            throw removalUnsupported();
        }

        @Override
        public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
            throw modificationUnsupported();
        }

        @Override
        public void ensureCapacity(final long tableSize) {
            resultColumn.ensureCapacity(tableSize);
        }

        @Override
        public Map<String, ? extends ColumnSource<?>> getResultColumns() {
            return Collections.singletonMap(resultName, resultColumn);
        }

        @Override
        public void propagateInitialState(@NotNull final QueryTable resultTable) {
            resultTable.getRowSet().forAllRowKeys(this::updateDestination);
        }

        @Override
        public void startTrackingPrevValues() {
            resultColumn.startTrackingPrevValues();
        }

        @Override
        public void propagateUpdates(@NotNull final TableUpdate downstream, @NotNull final RowSet newDestinations) {
            downstream.added().forAllRowKeys(this::updateDestination);
            if (modifiedThisStep) {
                downstream.modified().forAllRowKeys(this::updateDestination);
            }
        }

        private void updateDestination(final long destination) {
            resultColumn.set(destination, digestForSlot(destination).quantile(percentile));
        }
    }
}
