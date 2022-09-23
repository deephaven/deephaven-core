package io.deephaven.engine.table.impl.updateby.ema;

import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.prod.ShortCumProdOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class FloatEMAOperator extends BasePrimitiveEMAOperator {
    private final ColumnSource<Float> valueSource;

    protected class Context extends BasePrimitiveEMAOperator.Context {
        public FloatChunk<Values> floatValueChunk;

        protected Context(int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void storeValuesChunk(@NotNull final Chunk<Values> valuesChunk) {
            floatValueChunk = valuesChunk.asFloatChunk();
        }
    }

    /**
     * An operator that computes an EMA from a float column using an exponential decay function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param control        defines how to handle {@code null} input values.
     * @param timeScaleUnits the smoothing window for the EMA. If no {@code timeRecorder} is provided, this is measured
     *                       in ticks, otherwise it is measured in nanoseconds
     * @param valueSource the input column source.  Used when determining reset positions for reprocessing
     */
    public FloatEMAOperator(@NotNull final MatchPair pair,
                            @NotNull final String[] affectingColumns,
                            @NotNull final OperationControl control,
                            @Nullable final String timestampColumnName,
                            final long timeScaleUnits,
                            @NotNull final ColumnSource<Float> valueSource,
                            @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                            // region extra-constructor-args
                            // endregion extra-constructor-args
                            ) {
        super(pair, affectingColumns, control, timestampColumnName, timeScaleUnits, redirContext);
        this.valueSource = valueSource;
        // region constructor
        // endregion constructor
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize) {
        return new Context(chunkSize);
    }

    @Override
    public void push(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;

        final float input = ctx.floatValueChunk.get(pos);
        if (timestampColumnName == null) {
            // compute with ticks
            if(input == NULL_FLOAT) {
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
            final boolean isNull = input == NULL_FLOAT;
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
                        final double alpha = Math.exp(-dt / timeScaleUnits);
                        ctx.curVal = alpha * ctx.curVal + ((1 - alpha) * input);
                        ctx.lastStamp = timestamp;
                    }
                }
            }
        }
    }

    @Override
    public boolean isValueValid(long atKey) {
        final float value = valueSource.getFloat(atKey);
        if(value == NULL_FLOAT) {
            return false;
        }

        // Note that we don't care about Reset because in that case the current EMA at this key would be null
        // and the superclass will do the right thing.
        return !Float.isNaN(value) || control.onNanValueOrDefault() != BadDataBehavior.SKIP;
    }
}
