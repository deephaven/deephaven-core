package io.deephaven.engine.table.impl.updateby.emsum;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class BigDecimalEMSOperator extends BaseBigNumberEMSOperator<BigDecimal> {
    public class Context extends BaseBigNumberEMSOperator<BigDecimal>.Context {
        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void accumulateCumulative(RowSequence inputKeys,
                Chunk<? extends Values>[] valueChunkArr,
                LongChunk<? extends Values> tsChunk,
                int len) {
            setValuesChunk(valueChunkArr[0]);

            // chunk processing
            if (timestampColumnName == null) {
                // compute with ticks
                for (int ii = 0; ii < len; ii++) {
                    // read the value from the values chunk
                    final BigDecimal input = objectValueChunk.get(ii);
                    if (input == null) {
                        handleBadData(this, true);
                    } else {
                        if (curVal == null) {
                            curVal = input;
                        } else {
                            // Compute EM Sum by adding the current value to the decayed previous value.
                            curVal = curVal.multiply(opAlpha, control.bigValueContextOrDefault())
                                    .add(input, control.bigValueContextOrDefault());
                        }
                    }
                    outputValues.set(ii, curVal);
                }
            } else {
                // compute with time
                for (int ii = 0; ii < len; ii++) {
                    // read the value from the values chunk
                    final BigDecimal input = objectValueChunk.get(ii);
                    final long timestamp = tsChunk.get(ii);
                    final boolean isNull = input == null;
                    final boolean isNullTime = timestamp == NULL_LONG;
                    if (isNull) {
                        handleBadData(this, isNull);
                    } else if (isNullTime) {
                        // no change to curVal and lastStamp
                    } else {
                        if (curVal == null) {
                            curVal = input;
                            lastStamp = timestamp;
                        } else {
                            final long dt = timestamp - lastStamp;
                            // alpha is dynamic based on time, but only recalculated when needed
                            if (dt != lastDt) {
                                alpha = computeAlpha(-dt, reverseWindowScaleUnits);
                                lastDt = dt;
                            }
                            // Compute EM Sum by adding the current value to the decayed previous value.
                            curVal = curVal.multiply(alpha, control.bigValueContextOrDefault())
                                    .add(input, control.bigValueContextOrDefault());
                            lastStamp = timestamp;
                        }
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
     * An operator that computes an EMA from a BigDecimal column using an exponential decay function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param rowRedirection the {@link RowRedirection} to use for dense output sources
     * @param control defines how to handle {@code null} input values.
     * @param timestampColumnName the name of the column containing timestamps for time-based calcuations
     * @param windowScaleUnits the smoothing window for the EMA. If no {@code timestampColumnName} is provided, this is
     *        measured in ticks, otherwise it is measured in nanoseconds
     * @param valueSource a reference to the input column source for this operation
     */
    public BigDecimalEMSOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final RowRedirection rowRedirection,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            final long windowScaleUnits,
            final ColumnSource<?> valueSource) {
        super(pair, affectingColumns, rowRedirection, control, timestampColumnName, windowScaleUnits, valueSource);
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int chunkSize) {
        return new Context(chunkSize);
    }
}
