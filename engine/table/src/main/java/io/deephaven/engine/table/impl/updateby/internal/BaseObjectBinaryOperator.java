package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class BaseObjectBinaryOperator<T> extends BaseObjectUpdateByOperator<T> {
    public BaseObjectBinaryOperator(@NotNull final Class<T> type,
                                    @NotNull final MatchPair pair,
                                    @NotNull final String[] affectingColumns,
                                    @Nullable final RowRedirection rowRedirection) {
        super(pair, affectingColumns, rowRedirection, type);
    }

    protected abstract T doOperation(T bucketCurVal, T chunkCurVal);

    // region Addition

    @Override
    public void addChunk(@NotNull final UpdateContext context,
                         @NotNull final Chunk<Values> values,
                         @NotNull final LongChunk<? extends RowKeys> keyChunk,
                         @NotNull final IntChunk<RowKeys> bucketPositions,
                         @NotNull final IntChunk<ChunkPositions> startPositions,
                         @NotNull final IntChunk<ChunkLengths> runLengths) {
        final ObjectChunk<T, Values> asObject = values.asObjectChunk();
        //noinspection unchecked
        final Context ctx = (Context) context;
        for(int runIdx = 0; runIdx < startPositions.size(); runIdx++) {
            final int runStart = startPositions.get(runIdx);
            final int runLength = runLengths.get(runIdx);
            final int bucketPosition = bucketPositions.get(runStart);

            ctx.curVal = bucketLastVal.get(bucketPosition);
            accumulate(asObject, ctx, runStart, runLength);
            bucketLastVal.set(bucketPosition, ctx.curVal);
        }

        //noinspection unchecked
        outputSource.fillFromChunkUnordered(ctx.fillContext.get(), ctx.outputValues.get(), (LongChunk<RowKeys>) keyChunk);
    }

    @Override
    protected void doAddChunk(@NotNull final Context ctx,
                              @NotNull final RowSequence inputKeys,
                              @NotNull final Chunk<Values> workingChunk,
                              final long groupPosition) {
        if(bucketLastVal != null) {
            ctx.curVal = bucketLastVal.get(groupPosition);
        } else {
            ctx.curVal = groupPosition == singletonGroup ? singletonVal : null;
        }

        accumulate(workingChunk.asObjectChunk(), ctx, 0, inputKeys.intSize());

        if(bucketLastVal != null) {
            bucketLastVal.set(groupPosition, ctx.curVal);
        } else {
            singletonGroup = groupPosition;
            singletonVal = ctx.curVal;
        }
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
