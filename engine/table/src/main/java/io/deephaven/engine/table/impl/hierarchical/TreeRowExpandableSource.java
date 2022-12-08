package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.DefaultChunkSource;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.util.BooleanUtils;
import org.jetbrains.annotations.NotNull;

import java.util.function.Predicate;

/**
 * {@link ChunkSource} that produces {@code byte} values for {@code booleans that correspond to whether a tree
 * table row is expandable.
 */
final class TreeRowExpandableSource implements DefaultChunkSource.WithPrev<Values> {

    private final ColumnSource<?> rowIdentifierSource;
    private final Predicate<Object> rowIdentifierExpandable;

    TreeRowExpandableSource(
            @NotNull final ColumnSource<?> rowIdentifierSource,
            @NotNull final Predicate<Object> rowIdentifierExpandable) {
        this.rowIdentifierSource = rowIdentifierSource;
        this.rowIdentifierExpandable = rowIdentifierExpandable;
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Byte;
    }

    @Override
    public void fillChunk(
            @NotNull final ChunkSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        fillChunkInternal(context, destination, rowSequence, false);
    }

    @Override
    public void fillPrevChunk(
            @NotNull final ChunkSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        fillChunkInternal(context, destination, rowSequence, true);
    }

    private void fillChunkInternal(
            @NotNull final ChunkSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence,
            final boolean usePrev) {
        if (rowSequence.isEmpty()) {
            destination.setSize(0);
            return;
        }

        final FillContext fc = (FillContext) context;

        final Chunk<? extends Values> identifiersRaw = usePrev
                ? rowIdentifierSource.getChunk(fc.rowIdentifierContext, rowSequence)
                : rowIdentifierSource.getPrevChunk(fc.rowIdentifierContext, rowSequence);
        final ObjectChunk<?, ? extends Values> identifiers = fc.identifierBoxer.box(identifiersRaw);

        final int size = rowSequence.intSize();
        final WritableByteChunk<? super Values> typedDestination = destination.asWritableByteChunk();
        for (int ri = 0; ri < size; ++ri) {
            typedDestination.set(ri, BooleanUtils.booleanAsByte(rowIdentifierExpandable.test(identifiers.get(ri))));
        }
    }

    private static final class FillContext implements ChunkSource.FillContext {

        private final GetContext rowIdentifierContext;
        private final ChunkBoxer.BoxerKernel identifierBoxer;

        private FillContext(
                final int chunkCapacity,
                @NotNull final ColumnSource<?> treeRowIdentifierSource,
                final SharedContext sharedContext) {
            rowIdentifierContext = treeRowIdentifierSource.makeGetContext(chunkCapacity, sharedContext);
            identifierBoxer = ChunkBoxer.getBoxer(treeRowIdentifierSource.getChunkType(), chunkCapacity);
        }

        @Override
        public void close() {
            rowIdentifierContext.close();
            identifierBoxer.close();
        }
    }

    @Override
    public ChunkSource.FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new FillContext(chunkCapacity, rowIdentifierSource, sharedContext);
    }
}
