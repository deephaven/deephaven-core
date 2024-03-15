//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.em;

import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class FloatEMOperator extends BasePrimitiveEMOperator {
    protected class Context extends BasePrimitiveEMOperator.Context {
        public FloatChunk<? extends Values> floatValueChunk;

        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void accumulateCumulative(
                @NotNull final RowSequence inputKeys,
                @NotNull final Chunk<? extends Values>[] valueChunkArr,
                final LongChunk<? extends Values> tsChunk,
                final int len) {
            setValueChunks(valueChunkArr);

            // chunk processing
            if (timestampColumnName == null) {
                // compute with ticks
                for (int ii = 0; ii < len; ii++) {
                    // read the value from the values chunk
                    final float input = floatValueChunk.get(ii);
                    final boolean isNull = input == NULL_FLOAT;
                    final boolean isNan = Float.isNaN(input);

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
                    final float input = floatValueChunk.get(ii);
                    final long timestamp = tsChunk.get(ii);
                    final boolean isNull = input == NULL_FLOAT;
                    final boolean isNan = Float.isNaN(input);
                    final boolean isNullTime = timestamp == NULL_LONG;
                    // Handle bad data first
                    if (isNull || isNan) {
                        handleBadData(this, isNull, isNan);
                    } else if (isNullTime) {
                        // no change to curVal and lastStamp
                    } else if (curVal == NULL_DOUBLE) {
                        // If the data looks good, and we have a null computed value, accept the current value
                        curVal = input;
                        lastStamp = timestamp;
                    } else {
                        final long dt = timestamp - lastStamp;
                        if (dt < 0) {
                            // negative time deltas are not allowed, throw an exception
                            throw new TableDataException("Timestamp values in UpdateBy operators must not decrease");
                        }
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
            floatValueChunk = valueChunks[0].asFloatChunk();
        }

        @Override
        public boolean isValueValid(long atKey) {
            final float value = valueSource.getFloat(atKey);
            if (value == NULL_FLOAT) {
                return false;
            }
            return !Float.isNaN(value) || control.onNanValueOrDefault() != BadDataBehavior.SKIP;
        }


        @Override
        public void push(int pos, int count) {
            throw new IllegalStateException("EMAOperator#push() is not used");
        }
    }

    /**
     * An operator that computes an EMA from a float column using an exponential decay function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param control defines how to handle {@code null} input values.
     * @param timestampColumnName the name of the column containing timestamps for time-based calcuations
     * @param windowScaleUnits the smoothing window for the EMA. If no {@code timestampColumnName} is provided, this is
     *        measured in ticks, otherwise it is measured in nanoseconds
     */
    public FloatEMOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            final double windowScaleUnits,
            @NotNull final EmFunction aggFunction) {
        super(pair, affectingColumns, control, timestampColumnName, windowScaleUnits, aggFunction);
    }

    @Override
    public UpdateByOperator copy() {
        return new FloatEMOperator(
                pair,
                affectingColumns,
                control,
                timestampColumnName,
                reverseWindowScaleUnits,
                aggFunction);
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize);
    }
}
