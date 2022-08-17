/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortCumProdOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
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
import static io.deephaven.util.QueryConstants.NULL_INT;

public class IntCumProdOperator extends BaseLongUpdateByOperator {

    public IntCumProdOperator(final @NotNull MatchPair inputPair,
                                @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                // region extra-constructor-args
                                // endregion extra-constructor-args
    ) {
        super(inputPair, new String[]{ inputPair.rightColumn }, redirContext);
        // region constructor
        // endregion constructor
    }

    @Override
    protected void doAddChunk(@NotNull final Context ctx,
                              @NotNull final RowSequence inputKeys,
                              @NotNull final Chunk<Values> workingChunk) {
//        ctx.curVal = groupPosition == singletonGroup ? singletonVal : NULL_LONG;
        accumulate(workingChunk.asIntChunk(), ctx, 0, workingChunk.size());
//        singletonGroup = groupPosition;
//        singletonVal = ctx.curVal;
        outputSource.fillFromChunk(ctx.fillContext.get(), ctx.outputValues.get(), inputKeys);
    }

    private void accumulate(@NotNull final IntChunk<Values> asIntegers,
                            @NotNull final Context ctx,
                            final int runStart,
                            final int runLength) {
        final WritableLongChunk<Values> localOutputChunk = ctx.outputValues.get();
        for (int ii = runStart; ii < runStart + runLength; ii++) {
            final int currentVal = asIntegers.get(ii);
            final boolean isCurrentNull = currentVal == NULL_INT;;
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
