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
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


public class ObjectFillByOperator<T> extends BaseObjectUpdateByOperator<T> {
    // region extra-fields
    // endregion extra-fields

    public ObjectFillByOperator(@NotNull final MatchPair fillPair,
                              @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                              // region extra-constructor-args
                                      , final Class<T> colType
                              // endregion extra-constructor-args
                              ) {
        super(fillPair, new String[] { fillPair.rightColumn }, redirContext, colType);
        // region constructor
        // endregion constructor
    }

    // region extra-methods
    // endregion extra-methods

    @Override
    protected void doAddChunk(@NotNull final Context ctx,
                              @NotNull final RowSequence inputKeys,
                              @NotNull final Chunk<Values> workingChunk) {
        accumulate(workingChunk.asObjectChunk(), ctx, 0, workingChunk.size());
        outputSource.fillFromChunk(ctx.fillContext.get(), ctx.outputValues.get(), inputKeys);
    }

    private void accumulate(@NotNull final ObjectChunk<T, Values> asObjects,
                            @NotNull final Context ctx,
                            final int runStart,
                            final int runLength) {
        final WritableObjectChunk<T, Values> localOutputValues = ctx.outputValues.get();
        for (int ii = runStart; ii < runStart + runLength; ii++) {
            final T currentVal = asObjects.get(ii);
            if(currentVal != null) {
                ctx.curVal = currentVal;
            }
            localOutputValues.set(ii, ctx.curVal);
        }
    }
}
