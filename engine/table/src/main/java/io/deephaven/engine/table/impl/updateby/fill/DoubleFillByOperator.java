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
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DoubleFillByOperator extends BaseDoubleUpdateByOperator {
    // region extra-fields
    // endregion extra-fields

    public DoubleFillByOperator(@NotNull final MatchPair fillPair,
                              @Nullable final RowRedirection rowRedirection
                              // region extra-constructor-args
                              // endregion extra-constructor-args
                              ) {
        super(fillPair, new String[] { fillPair.rightColumn }, rowRedirection);
        // region constructor
        // endregion constructor
    }

    // region extra-methods
    // endregion extra-methods

    @Override
    public void addChunk(@NotNull final UpdateContext context,
                         @NotNull final Chunk<Values> values,
                         @NotNull final LongChunk<? extends RowKeys> keyChunk,
                         @NotNull final IntChunk<RowKeys> bucketPositions,
                         @NotNull final IntChunk<ChunkPositions> startPositions,
                         @NotNull final IntChunk<ChunkLengths> runLengths) {
        final DoubleChunk<Values> asDoubles = values.asDoubleChunk();
        final Context ctx = (Context) context;
        for(int runIdx = 0; runIdx < startPositions.size(); runIdx++) {
            final int runStart = startPositions.get(runIdx);
            final int runLength = runLengths.get(runIdx);
            final int bucketPosition = bucketPositions.get(runStart);

            ctx.curVal = bucketLastVal.getDouble(bucketPosition);
            accumulate(asDoubles, ctx, runStart, runLength);
            bucketLastVal.set(bucketPosition, ctx.curVal);
        }
        //noinspection unchecked
        outputSource.fillFromChunkUnordered(ctx.fillContext.get(), ctx.outputValues.get(), (LongChunk<RowKeys>) keyChunk);
    }

    @Override
    protected void doAddChunk(@NotNull final Context ctx,
                              @NotNull final RowSequence inputKeys,
                              @NotNull final Chunk<Values> workingChunk,
                              final long bucketPosition) {
        ctx.curVal = singletonGroup == bucketPosition ? singletonVal : NULL_DOUBLE;
        accumulate(workingChunk.asDoubleChunk(), ctx, 0, workingChunk.size());
        singletonGroup = bucketPosition;
        singletonVal = ctx.curVal;
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
