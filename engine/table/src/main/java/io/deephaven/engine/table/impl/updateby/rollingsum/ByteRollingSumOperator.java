/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortRollingSumOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedLongUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.QueryConstants.NULL_BYTE;

public class ByteRollingSumOperator extends BaseWindowedLongUpdateByOperator {
    // region extra-fields
    final byte nullValue;
    // endregion extra-fields

    protected class Context extends BaseWindowedLongUpdateByOperator.Context {
        public ByteChunk<Values> byteInfluencerValuesChunk;

        protected Context(int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void storeInfluencerValuesChunk(@NotNull final Chunk<Values> influencerValuesChunk) {
            byteInfluencerValuesChunk = influencerValuesChunk.asByteChunk();
        }
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(final int chunkSize, ColumnSource<?> inputSource) {
        return new Context(chunkSize);
    }

    public ByteRollingSumOperator(@NotNull final MatchPair pair,
                                   @NotNull final String[] affectingColumns,
                                   @NotNull final OperationControl control,
                                   @Nullable final String timestampColumnName,
                                   final long reverseTimeScaleUnits,
                                   final long forwardTimeScaleUnits,
                                   @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                   // region extra-constructor-args
                               ,final byte nullValue
                                   // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, control, timestampColumnName, reverseTimeScaleUnits, forwardTimeScaleUnits, redirContext);
        // region constructor
        this.nullValue = nullValue;
        // endregion constructor
    }

    @Override
    public void push(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;
        byte val = ctx.byteInfluencerValuesChunk.get(pos);

        // increase the running sum
        if (val != NULL_BYTE) {
            if (ctx.curVal == NULL_LONG) {
                ctx.curVal = val;
            } else {
                ctx.curVal += val;
            }
        } else {
            ctx.nullCount++;
        }
    }

    @Override
    public void pop(UpdateContext context) {
        final Context ctx = (Context) context;
        int pos = ctx.windowIndices.front();
        byte val = ctx.byteInfluencerValuesChunk.get(pos);

        // reduce the running sum
        if (val != NULL_BYTE) {
            ctx.curVal -= val;
        } else {
            ctx.nullCount--;
        }
    }
}
