package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.UnorderedRowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.DefaultChunkSource;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import io.deephaven.util.BooleanUtils;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ChunkSource} that produces {@code byte} values for {@link Boolean Booleans} that correspond to whether a tree
 * table row is expandable.
 */
final class TreeRowExpandableSource implements DefaultChunkSource.WithPrev<Values> {

    private final TreeTableImpl treeTable;
    private final ColumnSource<?> rowIdentifierSource;
    private final FillUnordered nodeTableSource;

    private TreeRowExpandableSource(
            @NotNull final TreeTableImpl treeTable,
            @NotNull final ColumnSource<?> rowIdentifierSource) {
        this.treeTable = treeTable;
        this.rowIdentifierSource = rowIdentifierSource;
        Assert.assertion(FillUnordered.providesFillUnordered(treeTable.getTreeNodeTableSource()),
                "Tree node table source supports unordered reads");
        nodeTableSource = (FillUnordered) treeTable.getTreeNodeTableSource();
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
        for (int ri = 0; ri < size; ++ri) {
            fc.nodeIds.set(ri, treeTable.nodeKeyToNodeId(identifiers.get(ri)));
        }

        if (usePrev) {
            nodeTableSource.fillPrevChunkUnordered(fc.nodeTableContext, fc.nodeTables, fc.nodeIds);
        } else {
            nodeTableSource.fillChunkUnordered(fc.nodeTableContext, fc.nodeTables, fc.nodeIds);
        }

        final WritableByteChunk<? super Values> typedDestination = destination.asWritableByteChunk();
        for (int ri = 0; ri < size; ++ri) {
            final Table nodeTable = fc.nodeTables.get(ri);
            final boolean expandable = nodeTable != null
                    && (usePrev ? nodeTable.getRowSet().sizePrev() : nodeTable.size()) > 0;
            // NB: Correctness-wise, we should actually be testing the result after node-level filtering.
            typedDestination.set(ri, BooleanUtils.booleanAsByte(expandable));
        }
    }

    private static final class FillContext implements ChunkSource.FillContext {

        private final GetContext rowIdentifierContext;
        private final ChunkBoxer.BoxerKernel identifierBoxer;
        private final WritableLongChunk<UnorderedRowKeys> nodeIds;
        private final ChunkSource.FillContext nodeTableContext;
        private final WritableObjectChunk<? extends Table, Values> nodeTables;

        private FillContext(
                final int chunkCapacity,
                @NotNull final ColumnSource<?> treeRowIdentifierSource,
                @NotNull final ColumnSource<? extends Table> nodeTableSource,
                final SharedContext sharedContext) {
            rowIdentifierContext = treeRowIdentifierSource.makeGetContext(chunkCapacity, sharedContext);
            identifierBoxer = ChunkBoxer.getBoxer(treeRowIdentifierSource.getChunkType(), chunkCapacity);
            nodeIds = WritableLongChunk.makeWritableChunk(chunkCapacity);
            nodeTableContext = nodeTableSource.makeFillContext(chunkCapacity, sharedContext);
            nodeTables = WritableObjectChunk.makeWritableChunk(chunkCapacity);
        }

        @Override
        public void close() {
            rowIdentifierContext.close();
            identifierBoxer.close();
            nodeIds.close();
            nodeTableContext.close();
            nodeTables.close();
        }
    }

    @Override
    public ChunkSource.FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new FillContext(chunkCapacity, rowIdentifierSource, treeTable.getTreeNodeTableSource(), sharedContext);
    }
}
