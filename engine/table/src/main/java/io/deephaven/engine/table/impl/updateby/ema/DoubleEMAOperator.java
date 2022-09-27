/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit FloatEMAOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.ema;

import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class DoubleEMAOperator extends BasePrimitiveEMAOperator {
    protected class Context extends BasePrimitiveEMAOperator.Context {
        private final ColumnSource<?> valueSource;

        public DoubleChunk<Values> doubleValueChunk;

        protected Context(int chunkSize, ColumnSource<?> inputSource) {
            super(chunkSize);
            this.valueSource = inputSource;
        }

        @Override
        public void storeValuesChunk(@NotNull final Chunk<Values> valuesChunk) {
            doubleValueChunk = valuesChunk.asDoubleChunk();
        }

        @Override
        public boolean isValueValid(long atKey) {
            final double value = valueSource.getDouble(atKey);
            if(value == NULL_DOUBLE) {
                return false;
            }

            // Note that we don't care about Reset because in that case the current EMA at this key would be null
            // and the superclass will do the right thing.
            return !Double.isNaN(value) || control.onNanValueOrDefault() != BadDataBehavior.SKIP;
        }
    }

    /**
     * An operator that computes an EMA from a double column using an exponential decay function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param control        defines how to handle {@code null} input values.
     * @param timeScaleUnits the smoothing window for the EMA. If no {@code timeRecorder} is provided, this is measured
     *                       in ticks, otherwise it is measured in nanoseconds
     */
    public DoubleEMAOperator(@NotNull final MatchPair pair,
                            @NotNull final String[] affectingColumns,
                            @NotNull final OperationControl control,
                            @Nullable final String timestampColumnName,
                            final long timeScaleUnits,
                            @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                            // region extra-constructor-args
                            // endregion extra-constructor-args
                            ) {
        super(pair, affectingColumns, control, timestampColumnName, timeScaleUnits, redirContext);
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

        final double input = ctx.doubleValueChunk.get(pos);
        final boolean isNull = input == NULL_DOUBLE;
        final boolean isNan = Double.isNaN(input);

        if (timestampColumnName == null) {
            // compute with ticks
            if(isNull || isNan) {
                handleBadData(ctx, isNull, isNan, false);
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
            final boolean isNullTime = timestamp == NULL_LONG;


            // Handle bad data first
            if (isNull || isNan || isNullTime) {
                handleBadData(ctx, isNull, isNan, isNullTime);
            } else if (ctx.curVal == NULL_DOUBLE) {
                // If the data looks good, and we have a null ema,  just accept the current value
                ctx.curVal = input;
                ctx.lastStamp = timestamp;
            } else {
                final boolean currentPoisoned = Double.isNaN(ctx.curVal);
                if (currentPoisoned && ctx.lastStamp == NULL_LONG) {
                    // If the current EMA was a NaN, we should accept the first good timestamp so that
                    // we can handle reset behavior properly in the following else
                    ctx.lastStamp = timestamp;
                } else {
                    final long dt = timestamp - ctx.lastStamp;
                    if (dt <= 0) {
                        handleBadTime(ctx, dt);
                    } else if (!currentPoisoned) {
                        final double alpha = Math.exp(-dt / timeScaleUnits);
                        ctx.curVal = alpha * ctx.curVal + ((1 - alpha) * input);
                        ctx.lastStamp = timestamp;
                    }
                }
            }
        }
    }
}
