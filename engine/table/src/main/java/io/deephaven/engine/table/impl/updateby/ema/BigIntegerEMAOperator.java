package io.deephaven.engine.table.impl.updateby.ema;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class BigIntegerEMAOperator extends BigNumberEMAOperator<BigInteger> {
    public class Context extends BigNumberEMAOperator<BigInteger>.Context {
        protected Context(int chunkSize, ColumnSource<?> inputSource) {
            super(chunkSize, inputSource);
        }

        @Override
        public void push(long key, int pos) {
            final BigInteger input = objectValueChunk.get(pos);
            if (timestampColumnName == null) {
                // compute with ticks
                if (input == null) {
                    handleBadData(this, true, false);
                } else {
                    final BigDecimal decimalInput = new BigDecimal(input, control.bigValueContextOrDefault());
                    if(curVal == null) {
                        curVal = decimalInput;
                    } else {
                        curVal = curVal.multiply(alpha, control.bigValueContextOrDefault())
                                .add(decimalInput.multiply(oneMinusAlpha, control.bigValueContextOrDefault()),
                                        control.bigValueContextOrDefault());
                    }
                }
            } else {
                // compute with time
                final long timestamp = timestampValueChunk.get(pos);
                final boolean isNull = input == null;
                final boolean isNullTime = timestamp == NULL_LONG;
                if (isNull || isNullTime) {
                    handleBadData(this, isNull, isNullTime);
                } else {
                    final BigDecimal decimalInput = new BigDecimal(input, control.bigValueContextOrDefault());
                    if(curVal == null) {
                        curVal = decimalInput;
                        lastStamp = timestamp;
                    } else {
                        final long dt = timestamp - lastStamp;
                        if(dt <= 0) {
                            handleBadTime(this, dt);
                        } else {
                            // alpha is dynamic, based on time
                            BigDecimal alpha = BigDecimal.valueOf(Math.exp(-dt / timeScaleUnits));
                            BigDecimal oneMinusAlpha = BigDecimal.ONE.subtract(alpha, control.bigValueContextOrDefault());

                            curVal = curVal.multiply(alpha, control.bigValueContextOrDefault())
                                    .add(decimalInput.multiply(oneMinusAlpha, control.bigValueContextOrDefault()),
                                            control.bigValueContextOrDefault());
                            lastStamp = timestamp;
                        }
                    }
                }
            }
        }
    }    
    
    /**
     * An operator that computes an EMA from a BigInteger column using an exponential decay function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param control defines how to handle {@code null} input values.
     * @param timestampColumnName an optional timestamp column. If this is null, it will be assumed time is measured in
     *                            integer ticks.
     * @param timeScaleUnits the smoothing window for the EMA. If no {@code timeRecorder} is provided, this is measured
     *        in ticks, otherwise it is measured in nanoseconds
     * @param redirContext the row redirection context to use for the EMA
     */
    public BigIntegerEMAOperator(@NotNull final MatchPair pair,
                                 @NotNull final String[] affectingColumns,
                                 @NotNull final OperationControl control,
                                 @Nullable final String timestampColumnName,
                                 final long timeScaleUnits,
                                 @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
    ) {
        super(pair, affectingColumns, control, timestampColumnName, timeScaleUnits, redirContext);
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(final int chunkSize, ColumnSource<?> inputSource) {
        return new Context(chunkSize, inputSource);
    }
}
