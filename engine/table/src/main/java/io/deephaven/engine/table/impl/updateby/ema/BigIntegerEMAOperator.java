package io.deephaven.engine.table.impl.updateby.ema;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.internal.LongRecordingUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class BigIntegerEMAOperator extends BigNumberEMAOperator<BigInteger> {
    /**
     * An operator that computes an EMA from an int column using an exponential decay function.
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
    public BigIntegerEMAOperator(@NotNull final MatchPair pair,
                                 @NotNull final String[] affectingColumns,
                                 @NotNull final OperationControl control,
                                 @Nullable final LongRecordingUpdateByOperator timeRecorder,
                                 final long timeScaleUnits,
                                 @NotNull final ColumnSource<BigInteger> valueSource,
                                 @Nullable final RowRedirection rowRedirection
                                 // region extra-constructor-args
                                 // endregion extra-constructor-args
                                 ) {
        super(pair, affectingColumns, control, timeRecorder, timeScaleUnits, valueSource, rowRedirection);
        // region constructor
        // endregion constructor
    }

    @Override
    void computeWithTicks(final EmaContext ctx,
                          final ObjectChunk<BigInteger, Values> valueChunk,
                          final int chunkStart,
                          final int chunkEnd) {
        final WritableObjectChunk<BigDecimal, Values> localOutputChunk = ctx.outputValues.get();
        for (int ii = chunkStart; ii < chunkEnd; ii++) {
            final BigInteger input = valueChunk.get(ii);
            if(input == null) {
                handleBadData(ctx, true, false);
            } else {
                final BigDecimal decimalInput = new BigDecimal(input, control.bigValueContextOrDefault());
                if(ctx.curVal == null) {
                    ctx.curVal = decimalInput;
                } else {
                    ctx.curVal = ctx.curVal.multiply(ctx.alpha, control.bigValueContextOrDefault())
                            .add(decimalInput.multiply(
                                            BigDecimal.ONE.subtract(ctx.alpha, control.bigValueContextOrDefault()),
                                            control.bigValueContextOrDefault()),
                                    control.bigValueContextOrDefault());
                }
            }

            localOutputChunk.set(ii, ctx.curVal);
        }
    }

    @Override
    void computeWithTime(final EmaContext ctx,
                         final ObjectChunk<BigInteger, Values> valueChunk,
                         final int chunkStart,
                         final int chunkEnd) {
        final WritableObjectChunk<BigDecimal, Values> localOutputChunk = ctx.outputValues.get();
        for (int ii = chunkStart; ii < chunkEnd; ii++) {
            final BigInteger input = valueChunk.get(ii);
            //noinspection ConstantConditions
            final long timestamp = timeRecorder.getLong(ii);
            final boolean isNull = input == null;
            final boolean isNullTime = timestamp == NULL_LONG;
            if(isNull || isNullTime) {
                handleBadData(ctx, isNull, isNullTime);
            } else {
                final BigDecimal decimalInput = new BigDecimal(input, control.bigValueContextOrDefault());
                if(ctx.curVal == null) {
                    ctx.curVal = decimalInput;
                    ctx.lastStamp = timestamp;
                } else {
                    final long dt = timestamp - ctx.lastStamp;
                    if(dt <= 0) {
                        handleBadTime(ctx, dt);
                    } else {
                        ctx.alpha = BigDecimal.valueOf(Math.exp(-dt / timeScaleUnits));
                        ctx.curVal = ctx.curVal.multiply(ctx.alpha, control.bigValueContextOrDefault())
                                .add(decimalInput.multiply(BigDecimal.ONE.subtract(ctx.alpha, control.bigValueContextOrDefault()), control.bigValueContextOrDefault()),
                                        control.bigValueContextOrDefault());
                        ctx.lastStamp = timestamp;
                    }
                }
            }

            localOutputChunk.set(ii, ctx.curVal);
        }
    }
}
