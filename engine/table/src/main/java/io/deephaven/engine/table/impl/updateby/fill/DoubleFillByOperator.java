/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharFillByOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.fill;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DoubleFillByOperator extends BaseDoubleUpdateByOperator {
    // region extra-fields
    // endregion extra-fields

    public DoubleFillByOperator(@NotNull final MatchPair fillPair,
                              @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                              // region extra-constructor-args
                              // endregion extra-constructor-args
                              ) {
        super(fillPair, new String[] { fillPair.rightColumn }, redirContext);
        // region constructor
        // endregion constructor
    }

    // region extra-methods
    // endregion extra-methods

    @Override
    protected void doAddChunk(@NotNull final Context ctx,
                              @NotNull final RowSequence inputKeys,
                              @NotNull final Chunk<Values> workingChunk) {
        accumulate(workingChunk.asDoubleChunk(), ctx, 0, workingChunk.size());
        outputSource.fillFromChunk(ctx.fillContext.get(), ctx.outputValues.get(), inputKeys);
    }

    private void accumulate(@NotNull final DoubleChunk<Values> asDoubles,
                            @NotNull final Context ctx,
                            final int runStart,
                            final int runLength) {
        final WritableDoubleChunk<Values> localOutputValues = ctx.outputValues.get();
        for (int ii = runStart; ii < runStart + runLength; ii++) {
            final double currentVal = asDoubles.get(ii);
            if(currentVal != NULL_DOUBLE) {
                ctx.curVal = currentVal;
            }
            localOutputValues.set(ii, ctx.curVal);
        }
    }
}
