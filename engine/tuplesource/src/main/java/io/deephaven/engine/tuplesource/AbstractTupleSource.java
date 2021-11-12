package io.deephaven.engine.tuplesource;

import io.deephaven.engine.rftable.Context;
import io.deephaven.engine.rftable.DefaultChunkSource;
import io.deephaven.engine.rftable.SharedContext;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.chunk.*;
import io.deephaven.engine.structures.RowSequence;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public abstract class AbstractTupleSource<TUPLE_TYPE>
        implements TupleSource<TUPLE_TYPE>, DefaultChunkSource.WithPrev<Attributes.Values> {

    private final ColumnSource[] columnSources;
    private final List<ColumnSource> listColumnSources;

    public AbstractTupleSource(ColumnSource... columnSources) {
        this.columnSources = columnSources;
        this.listColumnSources = Collections.unmodifiableList(Arrays.asList(columnSources));
    }

    @Override
    public final List<ColumnSource> getColumnSources() {
        return listColumnSources;
    }

    @Override
    public final ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @Override
    public final FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
        return new TupleFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public final void fillChunk(@NotNull FillContext context,
            @NotNull WritableChunk<? super Attributes.Values> destination, @NotNull RowSequence rowSequence) {
        // noinspection unchecked
        TupleFillContext tupleFillContext = (TupleFillContext) context;
        GetContext[] getContexts = tupleFillContext.getContexts;
        Chunk<Attributes.Values>[] chunks = tupleFillContext.chunks;

        for (int i = 0; i < columnSources.length; ++i) {
            // noinspection unchecked
            chunks[i] = columnSources[i].getChunk(getContexts[i], rowSequence);
        }

        convertChunks(destination, rowSequence.intSize(), chunks);
    }

    @Override
    public final void fillPrevChunk(@NotNull FillContext context,
            @NotNull WritableChunk<? super Attributes.Values> destination, @NotNull RowSequence rowSequence) {
        // noinspection unchecked
        TupleFillContext tupleFillContext = (TupleFillContext) context;
        GetContext[] getContexts = tupleFillContext.getContexts;
        Chunk<Attributes.Values>[] chunks = tupleFillContext.chunks;

        for (int i = 0; i < columnSources.length; ++i) {
            // noinspection unchecked
            chunks[i] = columnSources[i].getPrevChunk(getContexts[i], rowSequence);
        }

        convertChunks(destination, rowSequence.intSize(), chunks);
    }

    protected abstract void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize,
            Chunk<Attributes.Values>[] chunks);

    class TupleFillContext implements FillContext {

        GetContext[] getContexts;
        Chunk<Attributes.Values>[] chunks;

        TupleFillContext(int chunkCapacity, SharedContext sharedContext) {

            this.getContexts = Stream.of(columnSources).map(cs -> cs.makeGetContext(chunkCapacity, sharedContext))
                    .toArray(GetContext[]::new);
            // noinspection unchecked
            this.chunks = new Chunk[columnSources.length];
        }

        @Override
        public void close() {
            Stream.of(getContexts).forEach(Context::close);
        }
    }
}
