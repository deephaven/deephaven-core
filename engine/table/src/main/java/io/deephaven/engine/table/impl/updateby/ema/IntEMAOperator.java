/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortEMAOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.ema;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.internal.LongRecordingUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class IntEMAOperator extends BasePrimitiveEMAOperator {
    private final ColumnSource<Integer> valueSource;

    /**
     * An operator that computes an EMA from a int column using an exponential decay function.
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
    public IntEMAOperator(@NotNull final MatchPair pair,
                            @NotNull final String[] affectingColumns,
                            @NotNull final OperationControl control,
                            @Nullable final LongRecordingUpdateByOperator timeRecorder,
                            final long timeScaleUnits,
                            @NotNull final ColumnSource<Integer> valueSource,
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
        final IntChunk<Values> asIntegers = valueChunk.asIntChunk();
        final WritableDoubleChunk<Values> localOutputChunk = ctx.outputValues.get();
        for (int ii = chunkStart; ii < chunkEnd; ii++) {
            final int input = asIntegers.get(ii);
            if(input == NULL_INT) {
                handleBadData(ctx, true, false, false);
            } else {
                if(ctx.curVal == NULL_DOUBLE) {
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
        final IntChunk<Values> asIntegers = valueChunk.asIntChunk();
        final WritableDoubleChunk<Values> localOutputChunk = ctx.outputValues.get();
        for (int ii = chunkStart; ii < chunkEnd; ii++) {
            final int input = asIntegers.get(ii);
            //noinspection ConstantConditions
            final long timestamp = timeRecorder.getLong(ii);
            final boolean isNull = input == NULL_INT;
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
            localOutputChunk.set(ii, ctx.curVal);
        }
    }

    @Override
    boolean isValueValid(long atKey) {
        return valueSource.getInt(atKey) != NULL_INT;
    }
}
