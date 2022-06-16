package io.deephaven.engine.table.impl.updateby.prod;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.QueryConstants.NULL_SHORT;

public class ShortCumProdOperator extends BaseLongUpdateByOperator {

    public ShortCumProdOperator(final @NotNull MatchPair inputPair,
                                @Nullable final RowRedirection rowRedirection
                                // region extra-constructor-args
                                // endregion extra-constructor-args
    ) {
        super(inputPair, new String[]{ inputPair.rightColumn }, rowRedirection);
        // region constructor
        // endregion constructor
    }

    @Override
    public void addChunk(final @NotNull UpdateContext context,
                         final @NotNull Chunk<Values> values,
                         final @NotNull LongChunk<? extends RowKeys> keyChunk,
                         final @NotNull IntChunk<RowKeys> bucketPositions,
                         final @NotNull IntChunk<ChunkPositions> startPositions,
                         final @NotNull IntChunk<ChunkLengths> runLengths) {

        final Context ctx = (Context) context;
        final ShortChunk<Values> asShorts = values.asShortChunk();
        for(int runIdx = 0; runIdx < startPositions.size(); runIdx++) {
            final int runStart = startPositions.get(runIdx);
            final int runLength = runLengths.get(runIdx);
            final int bucketPosition = bucketPositions.get(runStart);

            ctx.curVal = bucketLastVal.getLong(bucketPosition);
            accumulate(asShorts, ctx, runStart, runLength);
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
        ctx.curVal = groupPosition == singletonGroup ? singletonVal : NULL_LONG;
        accumulate(workingChunk.asShortChunk(), ctx, 0, workingChunk.size());
        singletonGroup = groupPosition;
        singletonVal = ctx.curVal;
        outputSource.fillFromChunk(ctx.fillContext.get(), ctx.outputValues.get(), inputKeys);
    }

    private void accumulate(@NotNull final ShortChunk<Values> asShorts,
                            @NotNull final Context ctx,
                            final int runStart,
                            final int runLength) {
        final WritableLongChunk<Values> localOutputChunk = ctx.outputValues.get();
        for (int ii = runStart; ii < runStart + runLength; ii++) {
            final short currentVal = asShorts.get(ii);
            final boolean isCurrentNull = currentVal == NULL_SHORT;;
            if(ctx.curVal == NULL_LONG) {
                ctx.curVal = isCurrentNull ? NULL_LONG : currentVal;
            } else {
                if(!isCurrentNull) {
                    ctx.curVal *= currentVal;
                }
            }
            localOutputChunk.set(ii, ctx.curVal);
        }
    }
}
