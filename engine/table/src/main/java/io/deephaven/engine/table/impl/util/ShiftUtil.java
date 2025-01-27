package io.deephaven.engine.table.impl.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.WritableColumnSource;
import org.jetbrains.annotations.NotNull;

/**
 * Created by rbasralian on 1/24/25
 */
public class ShiftUtil {

    /**
     * Shift data within a WritableColumnSource. Be sure to {@link  LivenessScopeStack#open open()} a LivenessScope first.
     *
     * @param writableSource   The source
     * @param destContext      The
     * @param shiftContextSize
     * @param preMoveKeys
     * @param postMoveKeys
     * @param PAGE_SIZE
     */
    public static void applyShift(final @NotNull WritableColumnSource<?> writableSource,
                                  final @NotNull ChunkSink.FillFromContext destContext,
                                  final int shiftContextSize,
                                  final @NotNull RowSet preMoveKeys,
                                  final @NotNull RowSet postMoveKeys,
                                  final int PAGE_SIZE) {
        // note: we cannot use a get context here as destination is identical to source
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
}
