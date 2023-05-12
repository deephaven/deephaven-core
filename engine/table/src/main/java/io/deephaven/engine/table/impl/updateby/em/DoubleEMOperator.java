/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit FloatEMOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.em;

import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class DoubleEMOperator extends BasePrimitiveEMOperator {
    private final ColumnSource<?> valueSource;

    protected class Context extends BasePrimitiveEMOperator.Context {
        public DoubleChunk<? extends Values> doubleValueChunk;

        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void accumulateCumulative(RowSequence inputKeys,
                                         Chunk<? extends Values>[] valueChunkArr,
                                         LongChunk<? extends Values> tsChunk,
                                         int len) {
            setValueChunks(valueChunkArr);

            // chunk processing
            if (timestampColumnName == null) {
                // compute with ticks
                for (int ii = 0; ii < len; ii++) {
                    // read the value from the values chunk
                    final double input = doubleValueChunk.get(ii);
                    final boolean isNull = input == NULL_DOUBLE;
                    final boolean isNan = Double.isNaN(input);

                    if (isNull || isNan) {
                        handleBadData(this, isNull, isNan);
                    } else {
                        if (curVal == NULL_DOUBLE) {
                            curVal = input;
                        } else {
                            curVal = aggFunction.apply(curVal, input, opAlpha, opOneMinusAlpha);
                        }
                    }
                    outputValues.set(ii, curVal);
                }
            } else {
                // compute with time
                for (int ii = 0; ii < len; ii++) {
                    // read the value from the values chunk
                    final double input = doubleValueChunk.get(ii);
                    final long timestamp = tsChunk.get(ii);
                    final boolean isNull = input == NULL_DOUBLE;
                    final boolean isNan = Double.isNaN(input);
                    final boolean isNullTime = timestamp == NULL_LONG;
                    // Handle bad data first
                    if (isNull || isNan) {
                        handleBadData(this, isNull, isNan);
                    } else if (isNullTime) {
                        // no change to curVal and lastStamp
                    } else if (curVal == NULL_DOUBLE) {
                        // If the data looks good, and we have a null ema,  just accept the current value
                        curVal = input;
                        lastStamp = timestamp;
                    } else {
                        final long dt = timestamp - lastStamp;
                        if (dt != 0) {
                            final double alpha = Math.exp(-dt / reverseWindowScaleUnits);
                            final double oneMinusAlpha = 1.0 - alpha;
                            curVal = aggFunction.apply(curVal, input, alpha, oneMinusAlpha);
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
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            doubleValueChunk = valueChunks[0].asDoubleChunk();
        }

        @Override
        public boolean isValueValid(long atKey) {
            final double value = valueSource.getDouble(atKey);
            if (value == NULL_DOUBLE) {
                return false;
            }
            return !Double.isNaN(value) || control.onNanValueOrDefault() != BadDataBehavior.SKIP;
        }


        @Override
        public void push(int pos, int count) {
            throw new IllegalStateException("EMAOperator#push() is not used");
        }
    }

    /**
     * An operator that computes an EMA from a double column using an exponential decay function.
     *
     * @param pair                the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns    the names of the columns that affect this ema
     * @param rowRedirection      the {@link RowRedirection} to use for dense output sources
     * @param control             defines how to handle {@code null} input values.
     * @param timestampColumnName the name of the column containing timestamps for time-based calcuations
     * @param windowScaleUnits      the smoothing window for the EMA. If no {@code timestampColumnName} is provided, this is measured in ticks, otherwise it is measured in nanoseconds
     * @param valueSource         a reference to the input column source for this operation
     */
    public DoubleEMOperator(@NotNull final MatchPair pair,
                           @NotNull final String[] affectingColumns,
                           @Nullable final RowRedirection rowRedirection,
                           @NotNull final OperationControl control,
                           @Nullable final String timestampColumnName,
                           final double windowScaleUnits,
                           final ColumnSource<?> valueSource,
                           @NotNull final EmFunction aggFunction
                           // region extra-constructor-args
                           // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, control, timestampColumnName, windowScaleUnits, aggFunction);
        this.valueSource = valueSource;
        // region constructor
        // endregion constructor
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize);
    }
}
