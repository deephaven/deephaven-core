package io.deephaven.engine.table.impl.sources;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * This class wraps a {@link ColumnSource} of {@link TrackingRowSet} and returns {@link TrackingRowSet#prev()} when
 * previous values are requested. This should be used when the row set objects are mutated instead of replaced during a
 * cycle, e.g. when {@link Table#aggBy(Aggregation)} is used with {@link AggregationProcessor#forExposeGroupRowSets()}.
 */
public class RowSetColumnSourceWrapper extends AbstractColumnSource<RowSet> {
    private final ColumnSource<? extends TrackingRowSet> source;

    private static class GetContext implements ChunkSource.GetContext {
        private final ChunkSource.GetContext parentContext;
        protected WritableObjectChunk<RowSet, ? extends Values> localChunk;

        private GetContext(ChunkSource.GetContext sourceContext) {
            this.parentContext = sourceContext;
        }

        @Override
        public void close() {
            if (localChunk != null) {
                localChunk.close();
                localChunk = null;
            }
            parentContext.close();
        }
    }

    private RowSetColumnSourceWrapper(ColumnSource<? extends TrackingRowSet> source) {
        super(RowSet.class);
        this.source = source;
    }

    public static RowSetColumnSourceWrapper from(ColumnSource<? extends TrackingRowSet> source) {
        return new RowSetColumnSourceWrapper(source);
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull ColumnSource.GetContext context,
            @NotNull RowSequence rowSequence) {
        final GetContext ctx = (GetContext) context;
        return source.getChunk(ctx.parentContext, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull ColumnSource.GetContext context, long firstKey, long lastKey) {
        final GetContext ctx = (GetContext) context;
        return source.getChunk(ctx.parentContext, firstKey, lastKey);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        source.fillChunk(context, destination, rowSequence);
    }

    private WritableObjectChunk<RowSet, ? extends Values> ensureContextChunk(@NotNull GetContext context,
            int minCapacity) {
        if (context.localChunk != null && context.localChunk.capacity() >= minCapacity) {
            return context.localChunk;
        } else if (context.localChunk != null) {
            context.localChunk.close();
        }
        return context.localChunk = WritableObjectChunk.makeWritableChunk(minCapacity);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(@NotNull ChunkSource.GetContext context,
            @NotNull RowSequence rowSequence) {
        final GetContext ctx = (RowSetColumnSourceWrapper.GetContext) context;

        // Must return a chunk of the prev() values for each row in rowSequence
        ObjectChunk<TrackingRowSet, ? extends Values> chunk =
                (ObjectChunk<TrackingRowSet, ? extends Values>) source.getChunk(ctx.parentContext,
                        rowSequence);
        WritableObjectChunk<RowSet, ? extends Values> contextChunk = ensureContextChunk(ctx, chunk.size());

        for (int ii = 0; ii < chunk.size(); ii++) {
            contextChunk.set(ii, chunk.get(ii).prev());
        }
        return contextChunk;
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(@NotNull ColumnSource.GetContext context, long firstKey, long lastKey) {
        final GetContext ctx = (RowSetColumnSourceWrapper.GetContext) context;

        // Must return a chunk of the prev() values for each row in rowSequence
        ObjectChunk<TrackingRowSet, ? extends Values> chunk =
                (ObjectChunk<TrackingRowSet, ? extends Values>) source.getChunk(ctx.parentContext, firstKey,
                        lastKey);
        WritableObjectChunk<RowSet, ? extends Values> contextChunk = ensureContextChunk(ctx, chunk.size());

        for (int ii = 0; ii < chunk.size(); ii++) {
            contextChunk.set(ii, chunk.get(ii).prev());
        }
        return contextChunk;
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        // Must fill a chunk with the prev() values for each row in rowSequence
        source.fillChunk(context, destination, rowSequence);
        WritableObjectChunk localChunk = (WritableObjectChunk) destination;
        for (int ii = 0; ii < localChunk.size(); ii++) {
            // Replace each row set with its prev() row set
            localChunk.set(ii, ((TrackingRowSet) localChunk.get(ii)).prev());
        }
    }

    @Override
    public Class<RowSet> getType() {
        return RowSet.class;
    }

    @Override
    public Class<?> getComponentType() {
        return null;
    }

    @Override
    public WritableRowSet match(
            boolean invertMatch,
            boolean usePrev,
            boolean caseInsensitive,
            @Nullable final DataIndex dataIndex,
            @NotNull RowSet mapper, Object... keys) {
        throw new NotImplementedException("RowSetColumnSourceWrapper.match");
    }

    @Override
    public boolean isImmutable() {
        return source.isImmutable();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return false;
    }

    @Override
    public @Nullable RowSet get(long rowKey) {
        return source.get(rowKey);
    }

    @Override
    public @Nullable Boolean getBoolean(long rowKey) {
        throw new UnsupportedOperationException("RowSetColumnSourceWrapper.getBoolean");
    }

    @Override
    public byte getByte(long rowKey) {
        throw new UnsupportedOperationException("RowSetColumnSourceWrapper.getBoolean");
    }

    @Override
    public char getChar(long rowKey) {
        throw new UnsupportedOperationException("RowSetColumnSourceWrapper.getBoolean");
    }

    @Override
    public double getDouble(long rowKey) {
        throw new UnsupportedOperationException("RowSetColumnSourceWrapper.getBoolean");
    }

    @Override
    public float getFloat(long rowKey) {
        throw new UnsupportedOperationException("RowSetColumnSourceWrapper.getBoolean");
    }

    @Override
    public int getInt(long rowKey) {
        throw new UnsupportedOperationException("RowSetColumnSourceWrapper.getBoolean");
    }

    @Override
    public long getLong(long rowKey) {
        throw new UnsupportedOperationException("RowSetColumnSourceWrapper.getBoolean");
    }

    @Override
    public short getShort(long rowKey) {
        throw new UnsupportedOperationException("RowSetColumnSourceWrapper.getBoolean");
    }

    @Override
    public @Nullable RowSet getPrev(long rowKey) {
        // Return the prev() row set for TrackingRowSet at this row key
        return source.get(rowKey).prev();
    }

    @Override
    public @Nullable Boolean getPrevBoolean(long rowKey) {
        throw new UnsupportedOperationException("RowSetColumnSourceWrapper.getPrevBoolean");
    }

    @Override
    public byte getPrevByte(long rowKey) {
        throw new UnsupportedOperationException("RowSetColumnSourceWrapper.getPrevByte");
    }

    @Override
    public char getPrevChar(long rowKey) {
        throw new UnsupportedOperationException("RowSetColumnSourceWrapper.getPrevChar");
    }

    @Override
    public double getPrevDouble(long rowKey) {
        throw new UnsupportedOperationException("RowSetColumnSourceWrapper.getPrevDouble");
    }

    @Override
    public float getPrevFloat(long rowKey) {
        throw new UnsupportedOperationException("RowSetColumnSourceWrapper.getPrevFloat");
    }

    @Override
    public int getPrevInt(long rowKey) {
        throw new UnsupportedOperationException("RowSetColumnSourceWrapper.getPrevInt");
    }

    @Override
    public long getPrevLong(long rowKey) {
        throw new UnsupportedOperationException("RowSetColumnSourceWrapper.getPrevLong");
    }

    @Override
    public short getPrevShort(long rowKey) {
        throw new UnsupportedOperationException("RowSetColumnSourceWrapper.getPrevShort");
    }

    @Override
    public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
        return source.makeFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
        return new GetContext(source.makeGetContext(chunkCapacity, sharedContext));
    }

    @Override
    public List<ColumnSource<?>> getColumnSources() {
        return null;
    }
}
