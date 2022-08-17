package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class BaseObjectBinaryOperator<T> extends BaseObjectUpdateByOperator<T> {
    public BaseObjectBinaryOperator(@NotNull final Class<T> type,
                                    @NotNull final MatchPair pair,
                                    @NotNull final String[] affectingColumns,
                                    @NotNull final UpdateBy.UpdateByRedirectionContext redirContext) {
        super(pair, affectingColumns, redirContext, type);
    }

    protected abstract T doOperation(T bucketCurVal, T chunkCurVal);

    // region Addition


    @Override
    protected void doAddChunk(@NotNull final Context ctx,
                              @NotNull final RowSequence inputKeys,
                              @NotNull final Chunk<Values> workingChunk) {
        accumulate(workingChunk.asObjectChunk(), ctx, 0, inputKeys.intSize());
        outputSource.fillFromChunk(ctx.fillContext.get(), ctx.outputValues.get(), inputKeys);
    }

    // endregion

    private void accumulate(@NotNull final ObjectChunk<T, Values> asObject,
                              @NotNull final Context ctx,
                              final int runStart,
                              final int runLength) {
        final WritableObjectChunk<T, Values> localOutputValues = ctx.outputValues.get();
        for (int ii = runStart; ii < runStart + runLength; ii++) {
            final T currentVal = asObject.get(ii);
            if(ctx.curVal == null) {
                ctx.curVal = currentVal;
            } else if(currentVal != null) {
                ctx.curVal = doOperation(ctx.curVal, currentVal);
            }
            localOutputValues.set(ii, ctx.curVal);
        }
    }
}
