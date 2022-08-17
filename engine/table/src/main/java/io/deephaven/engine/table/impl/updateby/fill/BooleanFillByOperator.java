/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharFillByOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.fill;

import io.deephaven.engine.table.ColumnSource;
import java.util.Map;
import java.util.Collections;
import io.deephaven.engine.table.impl.sources.BooleanArraySource;
import io.deephaven.engine.table.impl.sources.BooleanSparseArraySource;
import io.deephaven.engine.table.WritableColumnSource;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseByteUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.BooleanUtils.NULL_BOOLEAN_AS_BYTE;

public class BooleanFillByOperator extends BaseByteUpdateByOperator {
    // region extra-fields
    // endregion extra-fields

    public BooleanFillByOperator(@NotNull final MatchPair fillPair,
                              @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                              // region extra-constructor-args
                              // endregion extra-constructor-args
                              ) {
        super(fillPair, new String[] { fillPair.rightColumn }, redirContext);
        // region constructor
        // endregion constructor
    }

    // region extra-methods
    @Override
    protected byte getNullValue() {
        return NULL_BOOLEAN_AS_BYTE;
    }
    @Override
    protected WritableColumnSource<Byte> makeSparseSource() {
        return (WritableColumnSource<Byte>) new BooleanSparseArraySource().reinterpret(byte.class);
    }

    @Override
    protected WritableColumnSource<Byte> makeDenseSource() {
        return (WritableColumnSource<Byte>) new BooleanArraySource().reinterpret(byte.class);
    }

    @NotNull
    @Override
    public Map<String, ColumnSource<?>> getOutputColumns() {
        return Collections.singletonMap(pair.leftColumn, outputSource.reinterpret(Boolean.class));
    }
    // endregion extra-methods

    @Override
    protected void doAddChunk(@NotNull final Context ctx,
                              @NotNull final RowSequence inputKeys,
                              @NotNull final Chunk<Values> workingChunk) {
        accumulate(workingChunk.asByteChunk(), ctx, 0, workingChunk.size());
        outputSource.fillFromChunk(ctx.fillContext.get(), ctx.outputValues.get(), inputKeys);
    }

    private void accumulate(@NotNull final ByteChunk<Values> asBooleans,
                            @NotNull final Context ctx,
                            final int runStart,
                            final int runLength) {
        final WritableByteChunk<Values> localOutputValues = ctx.outputValues.get();
        for (int ii = runStart; ii < runStart + runLength; ii++) {
            final byte currentVal = asBooleans.get(ii);
            if(currentVal != NULL_BOOLEAN_AS_BYTE) {
                ctx.curVal = currentVal;
            }
            localOutputValues.set(ii, ctx.curVal);
        }
    }
}
