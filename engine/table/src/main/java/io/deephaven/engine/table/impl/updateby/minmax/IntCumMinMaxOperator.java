/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortCumMinMaxOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.minmax;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseIntUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_INT;

public class IntCumMinMaxOperator extends BaseIntUpdateByOperator {
    private final boolean isMax;

    // region extra-fields
    // endregion extra-fields

    public IntCumMinMaxOperator(@NotNull final MatchPair inputPair,
                                  final boolean isMax,
                                  @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                  // region extra-constructor-args
                                  // endregion extra-constructor-args
                                  ) {
        super(inputPair, new String[] { inputPair.rightColumn }, redirContext);
        this.isMax = isMax;
        // region constructorÃ
        // endregion constructor
    }

    // region extra-methods
    // endregion extra-methods

    @Override
    protected void doProcessChunk(@NotNull final Context ctx,
                              @NotNull final RowSequence inputKeys,
                              @NotNull final Chunk<Values> workingChunk) {
        accumulate(workingChunk.asIntChunk(), ctx, 0, workingChunk.size());
        outputSource.fillFromChunk(ctx.fillContext.get(), ctx.outputValues.get(), inputKeys);
    }

    private void accumulate(@NotNull final IntChunk<Values> asIntegers,
                            @NotNull final Context ctx,
                            final int runStart,
                            final int runLength) {
        final WritableIntChunk<Values> localOutputValues = ctx.outputValues.get();
        for (int ii = runStart; ii < runStart + runLength; ii++) {
            final int currentVal = asIntegers.get(ii);
            if(ctx.curVal == NULL_INT) {
                ctx.curVal = currentVal;
            } else if(currentVal != NULL_INT) {
                if((isMax && currentVal > ctx.curVal) ||
                   (!isMax && currentVal < ctx.curVal)  ) {
                    ctx.curVal = currentVal;
                }
            }
            localOutputValues.set(ii, ctx.curVal);
        }
    }
}
