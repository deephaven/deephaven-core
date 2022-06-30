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
import io.deephaven.engine.table.impl.updateby.internal.BaseByteUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

public class ByteCumMinMaxOperator extends BaseByteUpdateByOperator {
    private final boolean isMax;

    // region extra-fields
    // endregion extra-fields

    public ByteCumMinMaxOperator(@NotNull final MatchPair inputPair,
                                  final boolean isMax,
                                  @Nullable final RowRedirection rowRedirection
                                  // region extra-constructor-args
                                  // endregion extra-constructor-args
                                  ) {
        super(inputPair, new String[] { inputPair.rightColumn }, rowRedirection);
        this.isMax = isMax;
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
        final ByteChunk<Values> asBytes = values.asByteChunk();
        final Context ctx = (Context) context;
        for(int runIdx = 0; runIdx < startPositions.size(); runIdx++) {
            final int runStart = startPositions.get(runIdx);
            final int runLength = runLengths.get(runIdx);
            final int bucketPosition = bucketPositions.get(runStart);

            ctx.curVal = bucketLastVal.getByte(bucketPosition);
            accumulate(asBytes, ctx, runStart, runLength);
            bucketLastVal.set(bucketPosition, ctx.curVal);
        }

        //noinspection unchecked
        outputSource.fillFromChunkUnordered(ctx.fillContext.get(), ctx.outputValues.get(), (LongChunk<RowKeys>) keyChunk);
    }

    @Override
    protected void doAddChunk(@NotNull final Context ctx,
                              @NotNull final RowSequence inputKeys,
                              @NotNull final Chunk<Values> workingChunk,
                              long groupPosition) {
        ctx.curVal = groupPosition == singletonGroup ? singletonVal : NULL_BYTE;
        accumulate(workingChunk.asByteChunk(), ctx, 0, workingChunk.size());
        singletonGroup = groupPosition;
        singletonVal = ctx.curVal;
        outputSource.fillFromChunk(ctx.fillContext.get(), ctx.outputValues.get(), inputKeys);
    }

    private void accumulate(@NotNull final ByteChunk<Values> asBytes,
                            @NotNull final Context ctx,
                            final int runStart,
                            final int runLength) {
        final WritableByteChunk<Values> localOutputValues = ctx.outputValues.get();
        for (int ii = runStart; ii < runStart + runLength; ii++) {
            final byte currentVal = asBytes.get(ii);
            if(ctx.curVal == NULL_BYTE) {
                ctx.curVal = currentVal;
            } else if(currentVal != NULL_BYTE) {
                if((isMax && currentVal > ctx.curVal) ||
                   (!isMax && currentVal < ctx.curVal)  ) {
                    ctx.curVal = currentVal;
                }
            }
            localOutputValues.set(ii, ctx.curVal);
        }
    }
}
