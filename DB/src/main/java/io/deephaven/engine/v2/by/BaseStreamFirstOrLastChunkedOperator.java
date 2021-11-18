package io.deephaven.engine.v2.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.*;
import io.deephaven.engine.v2.sources.*;
import io.deephaven.engine.chunk.*;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.lang.ref.SoftReference;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Base class with shared boilerplate for {@link StreamFirstChunkedOperator} and {@link StreamLastChunkedOperator}.
 */
public abstract class BaseStreamFirstOrLastChunkedOperator implements IterativeChunkedAggregationOperator {

    protected static final int COPY_CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    /**
     * The number of result columns. This is the size of {@link #resultColumns} and the length of {@link #inputColumns}
     * and {@link #outputColumns}.
     */
    protected final int numResultColumns;
    /**
     * Result columns, parallel to {@link #inputColumns} and {@link #outputColumns}.
     */
    protected final Map<String, ArrayBackedColumnSource<?>> resultColumns;
    /**
     * <p>
     * Input columns, parallel to {@link #outputColumns} and {@link #resultColumns}.
     * <p>
     * These are the source columns from the upstream table, reinterpreted to primitives where applicable.
     */
    protected final ColumnSource<?>[] inputColumns;
    /**
     * <p>
     * Output columns, parallel to {@link #inputColumns} and {@link #resultColumns}.
     * <p>
     * These are the result columns, reinterpreted to primitives where applicable.
     */
    protected final WritableColumnSource<?>[] outputColumns;
    /**
     * Cached pointer to the most-recently allocated {@link #redirections}.
     */
    protected SoftReference<LongArraySource> cachedRedirections;
    /**
     * Map from destination slot to first key. Only used during a step to keep track of the appropriate rows to copy
     * into the output columns.
     */
    protected LongArraySource redirections;

    protected BaseStreamFirstOrLastChunkedOperator(@NotNull final MatchPair[] resultPairs,
            @NotNull final Table streamTable) {
        numResultColumns = resultPairs.length;
        inputColumns = new ColumnSource[numResultColumns];
        outputColumns = new WritableColumnSource[numResultColumns];
        final Map<String, ArrayBackedColumnSource<?>> resultColumnsMutable = new LinkedHashMap<>(numResultColumns);
        for (int ci = 0; ci < numResultColumns; ++ci) {
            final MatchPair resultPair = resultPairs[ci];
            final ColumnSource<?> streamSource = streamTable.getColumnSource(resultPair.rightColumn());
            final ArrayBackedColumnSource<?> resultSource = ArrayBackedColumnSource.getMemoryColumnSource(0,
                    streamSource.getType(), streamSource.getComponentType());
            resultColumnsMutable.put(resultPair.leftColumn(), resultSource);
            inputColumns[ci] = ReinterpretUtil.maybeConvertToPrimitive(streamSource);
            // Note that ArrayBackedColumnSources implementations reinterpret very efficiently where applicable.
            outputColumns[ci] = (WritableColumnSource<?>) ReinterpretUtil.maybeConvertToPrimitive(resultSource);
            Assert.eq(inputColumns[ci].getChunkType(), "inputColumns[ci].getChunkType()",
                    outputColumns[ci].getChunkType(), "outputColumns[ci].getChunkType()");
        }
        resultColumns = Collections.unmodifiableMap(resultColumnsMutable);
        cachedRedirections = new SoftReference<>(redirections = new LongArraySource());
    }

    @Override
    public final Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return resultColumns;
    }

    @Override
    public final boolean requiresIndices() {
        return true;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void resetForStep(@NotNull final TableUpdate upstream) {
        if ((redirections = cachedRedirections.get()) == null) {
            cachedRedirections = new SoftReference<>(redirections = new LongArraySource());
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Unsupported / illegal operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public final void removeChunk(BucketedContext bucketedContext, Chunk<? extends Attributes.Values> values,
            LongChunk<? extends Attributes.RowKeys> inputIndices, IntChunk<Attributes.RowKeys> destinations,
            IntChunk<Attributes.ChunkPositions> startPositions, IntChunk<Attributes.ChunkLengths> length,
            WritableBooleanChunk<Attributes.Values> stateModified) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Attributes.Values> previousValues,
            Chunk<? extends Attributes.Values> newValues, LongChunk<? extends Attributes.RowKeys> postShiftIndices,
            IntChunk<Attributes.RowKeys> destinations, IntChunk<Attributes.ChunkPositions> startPositions,
            IntChunk<Attributes.ChunkLengths> length, WritableBooleanChunk<Attributes.Values> stateModified) {
        throw new IllegalStateException();
    }

    @Override
    public final void shiftChunk(BucketedContext bucketedContext, Chunk<? extends Attributes.Values> previousValues,
            Chunk<? extends Attributes.Values> newValues, LongChunk<? extends Attributes.RowKeys> preShiftIndices,
            LongChunk<? extends Attributes.RowKeys> postShiftIndices, IntChunk<Attributes.RowKeys> destinations,
            IntChunk<Attributes.ChunkPositions> startPositions, IntChunk<Attributes.ChunkLengths> length,
            WritableBooleanChunk<Attributes.Values> stateModified) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void modifyIndices(BucketedContext context, LongChunk<? extends Attributes.RowKeys> inputIndices,
            IntChunk<Attributes.RowKeys> destinations, IntChunk<Attributes.ChunkPositions> startPositions,
            IntChunk<Attributes.ChunkLengths> length, WritableBooleanChunk<Attributes.Values> stateModified) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean removeChunk(SingletonContext singletonContext, int chunkSize,
            Chunk<? extends Attributes.Values> values, LongChunk<? extends Attributes.RowKeys> inputIndices,
            long destination) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean modifyChunk(SingletonContext singletonContext, int chunkSize,
            Chunk<? extends Attributes.Values> previousValues, Chunk<? extends Attributes.Values> newValues,
            LongChunk<? extends Attributes.RowKeys> postShiftIndices, long destination) {
        throw new IllegalStateException();
    }

    @Override
    public final boolean shiftChunk(SingletonContext singletonContext,
            Chunk<? extends Attributes.Values> previousValues, Chunk<? extends Attributes.Values> newValues,
            LongChunk<? extends Attributes.RowKeys> preInputIndices,
            LongChunk<? extends Attributes.RowKeys> postInputIndices, long destination) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean modifyIndices(SingletonContext context, LongChunk<? extends Attributes.RowKeys> indices,
            long destination) {
        throw new UnsupportedOperationException();
    }
}
