package io.deephaven.engine.table.impl.updateby.ema;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.internal.LongRecordingUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class FloatEMAOperator extends BasePrimitiveEMAOperator {
    private final ColumnSource<Float> valueSource;

    /**
     * An operator that computes an EMA from a float column using an exponential decay function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param control        defines how to handle {@code null} input values.
     * @param timeRecorder   an optional recorder for a timestamp column.  If this is null, it will be assumed time is
     *                       measured in integer ticks.
     * @param timeScaleUnits the smoothing window for the EMA. If no {@code timeRecorder} is provided, this is measured
     *                       in ticks, otherwise it is measured in nanoseconds
     * @param valueSource the input column source.  Used when determining reset positions for reprocessing
     */
    public FloatEMAOperator(@NotNull final MatchPair pair,
                            @NotNull final String[] affectingColumns,
                            @NotNull final OperationControl control,
                            @Nullable final LongRecordingUpdateByOperator timeRecorder,
                            final long timeScaleUnits,
                            @NotNull final ColumnSource<Float> valueSource,
                            @Nullable final RowRedirection rowRedirection
                            // region extra-constructor-args
                            // endregion extra-constructor-args
                            ) {
        super(pair, affectingColumns, control, timeRecorder, timeScaleUnits, rowRedirection);
        this.valueSource = valueSource;
        // region constructor
        // endregion constructor
    }

    @Override
    void computeWithTicks(final EmaContext ctx,
                          final Chunk<Values> valueChunk,
                          final int chunkStart,
                          final int chunkEnd) {
        final FloatChunk<Values> asFloats = valueChunk.asFloatChunk();
        final WritableDoubleChunk<Values> localOutputChunk = ctx.outputValues.get();
        for (int ii = chunkStart; ii < chunkEnd; ii++) {
            final float input = asFloats.get(ii);
            final boolean isNull = input == NULL_FLOAT;
            final boolean isNan = Float.isNaN(input);
            if(isNull || isNan) {
                handleBadData(ctx, isNull, isNan, false);
            } else if(!Double.isNaN(ctx.curVal)) {
                if (ctx.curVal == NULL_DOUBLE) {
                    ctx.curVal = input;
                } else {
                    ctx.curVal = ctx.alpha * ctx.curVal + (ctx.oneMinusAlpha * input);
                }
            }

            localOutputChunk.set(ii, ctx.curVal);
        }
    }

    @Override
    void computeWithTime(final EmaContext ctx,
                         final Chunk<Values> valueChunk,
                         final int chunkStart,
                         final int chunkEnd) {
        final FloatChunk<Values> asFloats = valueChunk.asFloatChunk();
        final WritableDoubleChunk<Values> localOutputChunk = ctx.outputValues.get();
        for (int ii = chunkStart; ii < chunkEnd; ii++) {
            final float input = asFloats.get(ii);
            final long timestamp = timeRecorder.getLong(ii);
            final boolean isNull = input == NULL_FLOAT;
            final boolean isNan = Float.isNaN(input);
            final boolean isNullTime = timestamp == NULL_LONG;

            // Handle bad data first
            if(isNull || isNan || isNullTime) {
                handleBadData(ctx, isNull, isNan, isNullTime);
            } else if(ctx.curVal == NULL_DOUBLE) {
                // If the data looks good, and we have a null ema,  just accept the current value
                ctx.curVal = input;
                ctx.lastStamp = timestamp;
            } else {
                final boolean currentPoisoned = Double.isNaN(ctx.curVal);
                if(currentPoisoned && ctx.lastStamp == NULL_LONG) {
                    // If the current EMA was a NaN, we should accept the first good timestamp so that
                    // we can handle reset behavior properly in the following else
                    ctx.lastStamp = timestamp;
                } else {
                    final long dt = timestamp - ctx.lastStamp;
                    if(dt <= 0) {
                        handleBadTime(ctx, dt);
                    } else if(!currentPoisoned) {
                        final double alpha = Math.exp(-dt / timeScaleUnits);
                        ctx.curVal = alpha * ctx.curVal + ((1 - alpha) * input);
                        ctx.lastStamp = timestamp;
                    }
                }
            }

            localOutputChunk.set(ii, ctx.curVal);
        }
    }

    @Override
    boolean isValueValid(long atKey) {
        final float value = valueSource.getFloat(atKey);
        if(value == NULL_FLOAT) {
            return false;
        }

        // Note that we don't care about Reset because in that case the current EMA at this key would be null
        // and the superclass will do the right thing.
        return !Float.isNaN(value) || control.onNanValueOrDefault() != BadDataBehavior.SKIP;
    }
}
