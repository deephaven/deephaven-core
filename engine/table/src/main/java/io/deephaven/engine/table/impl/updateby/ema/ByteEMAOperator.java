/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortEMAOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.ema;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class ByteEMAOperator extends BasePrimitiveEMAOperator {

    protected class Context extends BasePrimitiveEMAOperator.Context {
        public final ColumnSource<?> valueSource;

        public ByteChunk<Values> byteValueChunk;

        protected Context(int chunkSize, ColumnSource<?> inputSource) {
            super(chunkSize);
            this.valueSource = inputSource;
        }

        @Override
        public void storeValuesChunk(@NotNull final Chunk<Values> valuesChunk) {
            byteValueChunk = valuesChunk.asByteChunk();
        }

        @Override
        public boolean isValueValid(long atKey) {
            return valueSource.getByte(atKey) != NULL_BYTE;
        }
    }

    /**
     * An operator that computes an EMA from a byte column using an exponential decay function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param control        defines how to handle {@code null} input values.
     * @param timeScaleUnits the smoothing window for the EMA. If no {@code timeRecorder} is provided, this is measured
     *                       in ticks, otherwise it is measured in nanoseconds
     */
    public ByteEMAOperator(@NotNull final MatchPair pair,
                            @NotNull final String[] affectingColumns,
                            @NotNull final OperationControl control,
                            @Nullable final String timestampColumnName,
                            final long timeScaleUnits,
                            @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                            // region extra-constructor-args
                            // endregion extra-constructor-args
                            ) {
        super(pair, affectingColumns, control, timestampColumnName, timeScaleUnits,redirContext);
        // region constructor
        // endregion constructor
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize, ColumnSource<?> inputSource) {
        return new Context(chunkSize, inputSource);
    }

    @Override
    public void push(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;

        final byte input = ctx.byteValueChunk.get(pos);
        if (timestampColumnName == null) {
            // compute with ticks
            if(input == NULL_BYTE) {
                handleBadData(ctx, true, false, false);
            } else {
                if(ctx.curVal == NULL_DOUBLE) {
                    ctx.curVal = input;
                } else {
                    ctx.curVal = alpha * ctx.curVal + (oneMinusAlpha * input);
                }
            }
        } else {
            // compute with time
            final long timestamp = ctx.timestampValueChunk.get(pos);
            //noinspection ConstantConditions
            final boolean isNull = input == NULL_BYTE;
            final boolean isNullTime = timestamp == NULL_LONG;
            if(isNull || isNullTime) {
                handleBadData(ctx, isNull, false, isNullTime);
            } else {
                if(ctx.curVal == NULL_DOUBLE) {
                    ctx.curVal = input;
                    ctx.lastStamp = timestamp;
                } else {
                    final long dt = timestamp - ctx.lastStamp;
                    if(dt <= 0) {
                        handleBadTime(ctx, dt);
                    } else {
                        // alpha is dynamic, based on time
                        final double alpha = Math.exp(-dt / timeScaleUnits);
                        ctx.curVal = alpha * ctx.curVal + ((1 - alpha) * input);
                        ctx.lastStamp = timestamp;
                    }
                }
            }
        }
    }
}
