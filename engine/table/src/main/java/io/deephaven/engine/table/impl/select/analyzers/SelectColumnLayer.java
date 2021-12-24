package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.sources.ChunkedBackingStoreExposedWritableSource;
import io.deephaven.time.DateTime;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.select.VectorChunkAdapter;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;

import java.util.function.LongToIntFunction;

final public class SelectColumnLayer extends SelectOrViewColumnLayer {
    /**
     * The same reference as super.columnSource, but as a WritableColumnSource
     */
    private final WritableColumnSource writableSource;
    private final boolean isRedirected;
    private final boolean flattenedResult;

    /**
     * A memoized copy of selectColumn's data view. Use {@link SelectColumnLayer#getChunkSource()} to access.
     */
    private ChunkSource<Values> chunkSource;

    SelectColumnLayer(SelectAndViewAnalyzer inner, String name, SelectColumn sc,
            WritableColumnSource ws, WritableColumnSource underlying,
            String[] deps, ModifiedColumnSet mcsBuilder, boolean isRedirected,
                      boolean flattenedResult) {
        super(inner, name, sc, ws, underlying, deps, mcsBuilder);
        this.writableSource = ws;
        this.isRedirected = isRedirected;
        this.flattenedResult = flattenedResult;
    }

    private ChunkSource<Values> getChunkSource() {
        if (chunkSource == null) {
            chunkSource = selectColumn.getDataView();
            if (selectColumnHoldsVector) {
                chunkSource = new VectorChunkAdapter<>(chunkSource);
            }
        }
        return chunkSource;
    }

    @Override
    public void applyUpdate(final TableUpdate upstream, final RowSet toClear,
            final UpdateHelper helper) {
        final int PAGE_SIZE = 4096;
        final LongToIntFunction contextSize = (long size) -> size > PAGE_SIZE ? PAGE_SIZE : (int) size;

        if (isRedirected && upstream.removed().isNonempty()) {
            clearObjectsAtThisLevel(upstream.removed());
        }

        // recurse so that dependent intermediate columns are already updated
        inner.applyUpdate(upstream, toClear, helper);

        final boolean modifiesAffectUs =
                upstream.modified().isNonempty() && upstream.modifiedColumnSet().containsAny(myModifiedColumnSet);

        // We include modifies in our shifted sets if we are not going to process them separately.
        final RowSet preMoveKeys = helper.getPreShifted(!modifiesAffectUs);
        final RowSet postMoveKeys = helper.getPostShifted(!modifiesAffectUs);

        final long lastKey = Math.max(postMoveKeys.isEmpty() ? -1 : postMoveKeys.lastRowKey(),
                upstream.added().isEmpty() ? -1 : upstream.added().lastRowKey());
        if (lastKey != -1) {
            if (flattenedResult) {
                // we know this only happens exactly one time, and we're going to flatten this
                writableSource.ensureCapacity(upstream.added().size());
            } else {
                writableSource.ensureCapacity(lastKey + 1);
            }
        }

        // Note that applyUpdate is called during initialization. If the table begins empty, we still want to force that
        // an initial call to getDataView() (via getChunkSource()) or else the formula will only be computed later when
        // data begins to flow; start-of-day is likely a bad time to find formula errors for our customers.
        final ChunkSource<Values> chunkSource = getChunkSource();

        final boolean needGetContext = upstream.added().isNonempty() || modifiesAffectUs;
        final boolean needDestContext = preMoveKeys.isNonempty() || needGetContext;
        final int chunkSourceContextSize =
                contextSize.applyAsInt(Math.max(upstream.added().size(), upstream.modified().size()));
        final int destContextSize = contextSize.applyAsInt(Math.max(preMoveKeys.size(), chunkSourceContextSize));
        final boolean isBackingChunkExposed = writableSource instanceof ChunkedBackingStoreExposedWritableSource;

        try (final ChunkSink.FillFromContext destContext =
                needDestContext ? writableSource.makeFillFromContext(destContextSize) : null;
                final ChunkSource.GetContext chunkSourceContext =
                        needGetContext ? chunkSource.makeGetContext(chunkSourceContextSize) : null;
                final ChunkSource.FillContext chunkSourceFillContext = needGetContext && isBackingChunkExposed ? chunkSource.makeFillContext(chunkSourceContextSize) : null) {

            // apply shifts!
            if (!isRedirected && preMoveKeys.isNonempty()) {
                assert !flattenedResult;
                assert destContext != null;
                // note: we cannot use a get context here as destination is identical to source
                final int shiftContextSize = contextSize.applyAsInt(preMoveKeys.size());
                try (final ChunkSource.FillContext srcContext = writableSource.makeFillContext(shiftContextSize);
                        final WritableChunk<Values> chunk =
                                writableSource.getChunkType().makeWritableChunk(shiftContextSize);
                        final RowSequence.Iterator srcIter = preMoveKeys.getRowSequenceIterator();
                        final RowSequence.Iterator destIter = postMoveKeys.getRowSequenceIterator()) {

                    while (srcIter.hasMore()) {
                        final RowSequence srcKeys = srcIter.getNextRowSequenceWithLength(PAGE_SIZE);
                        final RowSequence destKeys = destIter.getNextRowSequenceWithLength(PAGE_SIZE);
                        Assert.eq(srcKeys.size(), "srcKeys.size()", destKeys.size(), "destKeys.size()");
                        writableSource.fillPrevChunk(srcContext, chunk, srcKeys);
                        writableSource.fillFromChunk(destContext, chunk, destKeys);
                    }
                }
            }

            // apply adds!
            if (upstream.added().isNonempty()) {
                assert destContext != null;
                assert chunkSourceContext != null;
                if (isBackingChunkExposed) {
                    final ChunkedBackingStoreExposedWritableSource exposedWritableSource = (ChunkedBackingStoreExposedWritableSource) this.writableSource;
                    try (final RowSequence.Iterator keyIter = upstream.added().getRowSequenceIterator();
                         final RowSequence.Iterator destIter = flattenedResult ? RowSetFactory.flat(upstream.added().size()).getRowSequenceIterator() : null;
                         final ResettableWritableChunk<?> backingChunk = writableSource.getChunkType().makeResettableWritableChunk()) {
                        while (keyIter.hasMore()) {
                            final RowSequence keys = keyIter.getNextRowSequenceWithLength(PAGE_SIZE);
                            final RowSequence destKeys;
                            if (destIter != null) {
                                destKeys = destIter.getNextRowSequenceWithLength(PAGE_SIZE);
                            } else {
                                destKeys = keys;
                            }
                            if (keys.isContiguous() || flattenedResult) {
                                long rangeOffset = 0;
                                long firstDest = destKeys.firstRowKey();
                                final long lastDest = destKeys.lastRowKey();
                                while (firstDest < lastDest) {
                                    final long destCapacity = exposedWritableSource.resetWritableChunkToBackingStoreSlice(backingChunk, firstDest);
                                    if (destCapacity >= (lastDest - firstDest + 1)) {
                                        chunkSource.fillChunk(chunkSourceFillContext, backingChunk, keys);
                                    } else {
                                        try (RowSequence chunkSourceKeys = keys.getRowSequenceByPosition(rangeOffset, rangeOffset + destCapacity)) {
                                            chunkSource.fillChunk(chunkSourceFillContext, backingChunk, chunkSourceKeys);
                                        }
                                    }
                                    firstDest += destCapacity;
                                    rangeOffset += destCapacity;
                                }
                            } else {
                                writableSource.fillFromChunk(destContext, chunkSource.getChunk(chunkSourceContext, keys), destKeys);
                            }
                        }
                    }
                } else {
                    Assert.eqFalse(flattenedResult, "flattenedResult");
                    try (final RowSequence.Iterator keyIter = upstream.added().getRowSequenceIterator()) {
                        while (keyIter.hasMore()) {
                            final RowSequence keys = keyIter.getNextRowSequenceWithLength(PAGE_SIZE);
                            writableSource.fillFromChunk(destContext, chunkSource.getChunk(chunkSourceContext, keys), keys);
                        }
                    }
                }
            }

            // apply modifies!
            if (modifiesAffectUs) {
                assert !flattenedResult;
                assert chunkSourceContext != null;
                try (final RowSequence.Iterator keyIter = upstream.modified().getRowSequenceIterator()) {
                    while (keyIter.hasMore()) {
                        final RowSequence keys = keyIter.getNextRowSequenceWithLength(PAGE_SIZE);
                        writableSource.fillFromChunk(destContext, chunkSource.getChunk(chunkSourceContext, keys), keys);
                    }
                }
            }
        }

        if (!isRedirected) {
            clearObjectsAtThisLevel(toClear);
        }
    }

    private void clearObjectsAtThisLevel(RowSet keys) {
        // Only bother doing this if we're holding on to references.
        if (!writableSource.getType().isPrimitive() && (writableSource.getType() != DateTime.class)) {
            ChunkUtils.fillWithNullValue(writableSource, keys);
        }
    }

    @Override
    public boolean flattenedResult() {
        return flattenedResult;
    }
}
