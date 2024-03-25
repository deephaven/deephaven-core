//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedObjectChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This class wraps a {@link ColumnSource} of {@link RowSet} and returns {@link TrackingRowSet#prev()} when previous
 * values are requested and the accessed value is {@link TrackingRowSet tracking}. This should be used when the row set
 * objects are mutated instead of replaced during a cycle, e.g. when {@link Table#aggBy(Aggregation)} is used with
 * {@link AggregationProcessor#forExposeGroupRowSets()}.
 */
public class RowSetColumnSourceWrapper extends AbstractColumnSource<RowSet>
        implements MutableColumnSourceGetDefaults.ForObject<RowSet> {

    public static RowSetColumnSourceWrapper from(@NotNull final ColumnSource<? extends RowSet> source) {
        return new RowSetColumnSourceWrapper(source);
    }

    private final ColumnSource<? extends RowSet> source;

    private RowSetColumnSourceWrapper(@NotNull final ColumnSource<? extends RowSet> source) {
        super(RowSet.class);
        this.source = source;
    }

    private static class GetContext implements ChunkSource.GetContext {

        private final ChunkSource.GetContext sourceContext;

        private final SizedObjectChunk<? super RowSet, ? super Values> previousValues;

        private GetContext(@NotNull final ChunkSource.GetContext sourceContext) {
            this.sourceContext = sourceContext;
            previousValues = new SizedObjectChunk<>();
        }

        private WritableObjectChunk<? super RowSet, ? super Values> getPreviousValues(final int minCapacity) {
            previousValues.ensureCapacity(minCapacity);
            return previousValues.get();
        }

        @Override
        public void close() {
            SafeCloseable.closeAll(sourceContext, previousValues);
        }
    }

    private static RowSet maybeGetPrevValue(@Nullable final RowSet value) {
        return value != null && value.isTracking() ? value.trackingCast().prev() : value;
    }

    /**
     * Copy values from {@code source} to {@code destination}, re-mapping any {@link TrackingRowSet tracking} values to
     * {@link TrackingRowSet#prev()}.
     *
     * @param source The source chunk
     * @param destination The destination chunk
     * @apiNote {@code source} and {@code destination} may be the same chunk
     */
    private static void maybeCopyPrevValues(
            @NotNull final ObjectChunk<? extends RowSet, ? extends Values> source,
            @NotNull final WritableObjectChunk<? super RowSet, ? super Values> destination) {
        final int size = source.size();
        for (int ii = 0; ii < size; ii++) {
            destination.set(ii, maybeGetPrevValue(source.get(ii)));
        }
        destination.setSize(size);
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
    public @Nullable RowSet get(final long rowKey) {
        return source.get(rowKey);
    }

    @Override
    public @Nullable RowSet getPrev(final long rowKey) {
        return maybeGetPrevValue(source.getPrev(rowKey));
    }

    @Override
    public ChunkSource.GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new GetContext(source.makeGetContext(chunkCapacity, sharedContext));
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return source.makeFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public Chunk<? extends Values> getChunk(
            @NotNull final ColumnSource.GetContext context,
            @NotNull final RowSequence rowSequence) {
        return source.getChunk(((GetContext) context).sourceContext, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getChunk(
            @NotNull final ColumnSource.GetContext context,
            final long firstKey,
            final long lastKey) {
        return source.getChunk(((GetContext) context).sourceContext, firstKey, lastKey);
    }

    @Override
    public void fillChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        source.fillChunk(context, destination, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(
            @NotNull final ChunkSource.GetContext context,
            @NotNull final RowSequence rowSequence) {
        final GetContext typedContext = (GetContext) context;
        final ObjectChunk<? extends RowSet, ? extends Values> sourceChunk =
                source.getPrevChunk(typedContext.sourceContext, rowSequence).asObjectChunk();
        final WritableObjectChunk<? super RowSet, ? super Values> destination =
                typedContext.getPreviousValues(sourceChunk.size());
        maybeCopyPrevValues(sourceChunk, destination);
        return ObjectChunk.downcast(destination);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(
            @NotNull final ColumnSource.GetContext context,
            final long firstKey,
            final long lastKey) {
        final GetContext typedContext = (GetContext) context;
        final ObjectChunk<? extends RowSet, ? extends Values> sourceChunk =
                source.getPrevChunk(typedContext.sourceContext, firstKey, lastKey).asObjectChunk();
        final WritableObjectChunk<? super RowSet, ? super Values> destination =
                typedContext.getPreviousValues(sourceChunk.size());
        maybeCopyPrevValues(sourceChunk, destination);
        return ObjectChunk.downcast(destination);
    }

    @Override
    public void fillPrevChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        source.fillPrevChunk(context, destination, rowSequence);
        maybeCopyPrevValues(ObjectChunk.downcast(destination.asObjectChunk()), destination.asWritableObjectChunk());
    }
}
