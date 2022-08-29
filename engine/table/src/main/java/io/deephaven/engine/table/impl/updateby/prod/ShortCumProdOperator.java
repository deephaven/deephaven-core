package io.deephaven.engine.table.impl.updateby.prod;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.QueryConstants.NULL_SHORT;

public class ShortCumProdOperator extends BaseLongUpdateByOperator {

    public ShortCumProdOperator(final @NotNull MatchPair inputPair,
                                @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                // region extra-constructor-args
                                // endregion extra-constructor-args
    ) {
        super(inputPair, new String[]{ inputPair.rightColumn }, redirContext);
        // region constructor
        // endregion constructor
    }

    @Override
    protected void doProcessChunk(@NotNull final Context ctx,
                              @NotNull final RowSequence inputKeys,
                              @NotNull final Chunk<Values> workingChunk) {
//        ctx.curVal = groupPosition == singletonGroup ? singletonVal : NULL_LONG;
        accumulate(workingChunk.asShortChunk(), ctx, 0, workingChunk.size());
//        singletonGroup = groupPosition;
//        singletonVal = ctx.curVal;
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
