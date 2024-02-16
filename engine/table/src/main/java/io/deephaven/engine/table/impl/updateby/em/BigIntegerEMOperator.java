package io.deephaven.engine.table.impl.updateby.em;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class BigIntegerEMOperator extends BaseBigNumberEMOperator<BigInteger> {
    public class Context extends BaseBigNumberEMOperator<BigInteger>.Context {
        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void accumulateCumulative(
                @NotNull final RowSequence inputKeys,
                @NotNull final Chunk<? extends Values>[] valueChunkArr,
                @Nullable final LongChunk<? extends Values> tsChunk,
                final int len) {
            setValueChunks(valueChunkArr);

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
                            curVal = aggFunction.apply(curVal, decimalInput, opAlpha, opOneMinusAlpha);
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
                    } else if (curVal == null) {
                        // We have a valid input value, we can initialize the output value with it.
                        curVal = new BigDecimal(input, control.bigValueContextOrDefault());
                        lastStamp = timestamp;
                    } else {
                        final long dt = timestamp - lastStamp;
                        if (dt < 0) {
                            // negative time deltas are not allowed, throw an exception
                            throw new TableDataException("Timestamp values in UpdateBy operators must not decrease");
                        }
                        // Alpha is dynamic based on time, but only recalculated when needed
                        if (dt != lastDt) {
                            alpha = computeAlpha(-dt, reverseWindowScaleUnits);
                            oneMinusAlpha = computeOneMinusAlpha(alpha);
                            lastDt = dt;
                        }
                        final BigDecimal decimalInput = new BigDecimal(input, control.bigValueContextOrDefault());
                        curVal = aggFunction.apply(curVal, decimalInput, alpha, oneMinusAlpha);
                        lastStamp = timestamp;
                    }
                    outputValues.set(ii, curVal);
                }
            }

            // chunk output to column
            writeToOutputColumn(inputKeys);
        }

        @Override
        public void push(int pos, int count) {
            throw new IllegalStateException("EMAOperator#push() is not used");
        }
    }
    
    /**
     * An operator that computes an EMA from a BigInteger column using an exponential decay function.
     *
     * @param pair                the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns    the names of the columns that affect this ema
     * @param control             defines how to handle {@code null} input values.
     * @param timestampColumnName the name of the column containing timestamps for time-based calcuations
     * @param windowScaleUnits      the smoothing window for the EMA. If no {@code timestampColumnName} is provided, this is measured in ticks, otherwise it is measured in nanoseconds
     */
    public BigIntegerEMOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            final double windowScaleUnits,
            @NotNull final EmFunction aggFunction) {
        super(pair, affectingColumns, control, timestampColumnName, windowScaleUnits, aggFunction);
    }

    @Override
    public UpdateByOperator copy() {
        return new BigIntegerEMOperator(pair, affectingColumns, control, timestampColumnName, reverseWindowScaleUnits, aggFunction);
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize);
    }
}
