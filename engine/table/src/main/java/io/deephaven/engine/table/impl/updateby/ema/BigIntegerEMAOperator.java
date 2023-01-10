package io.deephaven.engine.table.impl.updateby.ema;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class BigIntegerEMAOperator extends BigNumberEMAOperator<BigInteger> {
    public class Context extends BigNumberEMAOperator<BigInteger>.Context {
        protected Context(int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void accumulate(RowSequence inputKeys,
                               Chunk<? extends Values>[] valueChunkArr,
                               LongChunk<? extends Values> tsChunk,
                               int len) {
            setValuesChunk(valueChunkArr[0]);
            setTimestampChunk(tsChunk);

            // chunk processing
            if (timestampColumnName == null) {
                // compute with ticks
                for (int ii = 0; ii < len; ii++) {
                    // read the value from the values chunk
                    final BigInteger input = objectValueChunk.get(ii);
                    if (input == null) {
                        handleBadData(this, true);
                    } else {
                        final BigDecimal decimalInput = new BigDecimal(input, control.bigValueContextOrDefault());
                        if (curVal == null) {
                            curVal = decimalInput;
                        } else {
                            curVal = curVal.multiply(alpha, control.bigValueContextOrDefault())
                                    .add(decimalInput.multiply(oneMinusAlpha, control.bigValueContextOrDefault()),
                                            control.bigValueContextOrDefault());
                        }
                    }
                    outputValues.set(ii, curVal);
                }
            } else {
                // compute with time
                for (int ii = 0; ii < len; ii++) {
                    // read the value from the values chunk
                    final BigInteger input = objectValueChunk.get(ii);
                    final long timestamp = tsChunk.get(ii);
                    final boolean isNull = input == null;
                    final boolean isNullTime = timestamp == NULL_LONG;
                    if (isNull) {
                        handleBadData(this, true);
                    } else if (isNullTime) {
                        // no change to curVal and lastStamp
                    } else {
                        final BigDecimal decimalInput = new BigDecimal(input, control.bigValueContextOrDefault());
                        if (curVal == null) {
                            curVal = decimalInput;
                            lastStamp = timestamp;
                        } else {
                            final long dt = timestamp - lastStamp;
                            if (dt == 0) {
                                // preserve curVal and timestamp
                            } else {
                                // alpha is dynamic, based on time
                                BigDecimal alpha = BigDecimal.valueOf(Math.exp(-dt / (double)reverseTimeScaleUnits));
                                BigDecimal oneMinusAlpha = BigDecimal.ONE.subtract(alpha, control.bigValueContextOrDefault());

                                curVal = curVal.multiply(alpha, control.bigValueContextOrDefault())
                                        .add(decimalInput.multiply(oneMinusAlpha, control.bigValueContextOrDefault()),
                                                control.bigValueContextOrDefault());
                                lastStamp = timestamp;
                            }
                        }
                    }
                    outputValues.set(ii, curVal);
                }
            }

            // chunk output to column
            writeToOutputColumn(inputKeys);
        }

        @Override
        public void push(long key, int pos) {
            throw new IllegalStateException("EMAOperator#push() is not used");
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
     * @param rowRedirection the row redirection for the EMA output column
     */
    public BigIntegerEMAOperator(@NotNull final MatchPair pair,
                                 @NotNull final String[] affectingColumns,
                                 @NotNull final OperationControl control,
                                 @Nullable final String timestampColumnName,
                                 final long timeScaleUnits,
                                 @Nullable final RowRedirection rowRedirection,
                                 final ColumnSource<?> valueSource) {
        super(pair, affectingColumns, control, timestampColumnName, timeScaleUnits, rowRedirection, valueSource);
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(final int chunkSize) {
        return new Context(chunkSize);
    }
}
