package io.deephaven.engine.table.impl.updateby.ema;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class BigDecimalEMAOperator extends BigNumberEMAOperator<BigDecimal> {

    /**
     * An operator that computes an EMA from a int column using an exponential decay function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param control defines how to handle {@code null} input values.
     * @param timestampColumnName an optional timestamp column. If this is null, it will be assumed time is measured in
     *        integer ticks.
     * @param timeScaleUnits the smoothing window for the EMA. If no {@code timeRecorder} is provided, this is measured
     *        in ticks, otherwise it is measured in nanoseconds
     * @param redirContext the row redirection context to use for the EMA
     */
    public BigDecimalEMAOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            final long timeScaleUnits,
            @NotNull final UpdateBy.UpdateByRedirectionContext redirContext) {
        super(pair, affectingColumns, control, timestampColumnName, timeScaleUnits, redirContext);
    }

    @Override
    public void push(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;
        final BigDecimal input = ctx.objectValueChunk.get(pos);
        if (timestampColumnName == null) {
            // compute with ticks
            if (input == null) {
                handleBadData(ctx, true, false);
            } else {
                if (ctx.curVal == null) {
                    ctx.curVal = input;
                } else {
                    ctx.curVal = ctx.curVal.multiply(alpha, control.bigValueContextOrDefault())
                            .add(input.multiply(oneMinusAlpha, control.bigValueContextOrDefault()),
                                    control.bigValueContextOrDefault());
                }
            }
        } else {
            // compute with time
            final long timestamp = ctx.timestampValueChunk.get(pos);
            final boolean isNull = input == null;
            final boolean isNullTime = timestamp == NULL_LONG;
            if (isNull || isNullTime) {
                handleBadData(ctx, isNull, isNullTime);
            } else {
                if (ctx.curVal == null) {
                    ctx.curVal = input;
                    ctx.lastStamp = timestamp;
                } else {
                    final long dt = timestamp - ctx.lastStamp;
                    if (dt <= 0) {
                        handleBadTime(ctx, dt);
                    } else {
                        // alpha is dynamic, based on time
                        BigDecimal alpha = BigDecimal.valueOf(Math.exp(-dt / timeScaleUnits));
                        BigDecimal oneMinusAlpha = BigDecimal.ONE.subtract(alpha, control.bigValueContextOrDefault());

                        ctx.curVal = ctx.curVal.multiply(alpha, control.bigValueContextOrDefault())
                                .add(input.multiply(oneMinusAlpha, control.bigValueContextOrDefault()),
                                        control.bigValueContextOrDefault());
                        ctx.lastStamp = timestamp;
                    }
                }
            }
        }
    }
}
