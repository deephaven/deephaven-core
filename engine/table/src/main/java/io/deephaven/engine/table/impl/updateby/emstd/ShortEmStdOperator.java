/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharEmStdOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.emstd;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.ShortChunk;
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

import static io.deephaven.util.QueryConstants.*;

public class ShortEmStdOperator extends BasePrimitiveEmStdOperator {
    public final ColumnSource<?> valueSource;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BasePrimitiveEmStdOperator.Context {

        public ShortChunk<? extends Values> shortValueChunk;

        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void accumulateCumulative(@NotNull RowSequence inputKeys,
                                         Chunk<? extends Values>[] valueChunkArr,
                                         LongChunk<? extends Values> tsChunk,
                                         int len) {
            setValuesChunk(valueChunkArr[0]);

            // chunk processing
            if (timestampColumnName == null) {
                // compute with ticks
                for (int ii = 0; ii < len; ii++) {
                    // read the value from the values chunk
                    final short input = shortValueChunk.get(ii);

                    if (input == NULL_SHORT) {
                        handleBadData(this, true, false);
                    } else {
                        if (curEma == NULL_DOUBLE) {
                            curEma = input;
                            curVariance = 0.0;
                            curVal = Double.NaN;
                        } else {
                            //  incremental variance = alpha * (prevVariance + (1 â alpha) * (x â prevEma)^2)
                            curVariance = opAlpha * (curVariance + opOneMinusAlpha * Math.pow(input - curEma, 2.0));

                            final double decayedEmaVal = curEma * opAlpha;
                            curEma =  decayedEmaVal + (opOneMinusAlpha * input);
                            curVal = Math.sqrt(curVariance);
                        }
                    }
                    outputValues.set(ii, curVal);
                    if (emaValues != null) {
                        emaValues.set(ii, curEma);
                    }
                }
            } else {
                // compute with time
                for (int ii = 0; ii < len; ii++) {
                    // read the value from the values chunk
                    final short input = shortValueChunk.get(ii);
                    final long timestamp = tsChunk.get(ii);
                    //noinspection ConstantConditions
                    final boolean isNull = input == NULL_SHORT;
                    final boolean isNullTime = timestamp == NULL_LONG;

                    if (isNull) {
                        handleBadData(this, true, false);
                    } else if (isNullTime) {
                        // no change to curVal and lastStamp
                    } else if (curEma == NULL_DOUBLE) {
                        curEma = input;
                        curVariance = 0.0;
                        curVal = Double.NaN;
                        lastStamp = timestamp;
                    } else {
                        final long dt = timestamp - lastStamp;
                        if (dt != lastDt) {
                            // Alpha is dynamic based on time, but only recalculated when needed
                            alpha = Math.exp(-dt / (double) reverseWindowScaleUnits);
                            oneMinusAlpha = 1.0 - alpha;
                            lastDt = dt;
                        }
                        //  incremental variance = alpha * (prevVariance + (1 â alpha) * (x â prevEma)^2)
                        curVariance = alpha * (curVariance + oneMinusAlpha * Math.pow(input - curEma, 2.0));

                        final double decayedEmaVal = curEma * alpha;
                        curEma =  decayedEmaVal + (oneMinusAlpha * input);
                        curVal = Math.sqrt(curVariance);

                        lastStamp = timestamp;
                    }
                    outputValues.set(ii, curVal);
                    if (emaValues != null) {
                        emaValues.set(ii, curEma);
                    }
                }
            }

            // chunk output to column
            writeToOutputColumn(inputKeys);
        }

        @Override
        public void writeToOutputColumn(@NotNull final RowSequence inputKeys) {
            outputSource.fillFromChunk(outputFillContext, outputValues, inputKeys);
            if (emaValues != null) {
                emaSource.fillFromChunk(emaFillContext, emaValues, inputKeys);
            }
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            shortValueChunk = valuesChunk.asShortChunk();
        }

        @Override
        public boolean isValueValid(long atKey) {
            return valueSource.getShort(atKey) != NULL_SHORT;
        }

        @Override
        public void push(int pos, int count) {
            throw new IllegalStateException("EMAOperator#push() is not used");
        }
    }

    /**
     * An operator that computes an exponential moving standard deviation from a short column using an exponential
     * decay function.
     *
     * @param pair                the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns    the names of the columns that affect this ema
     * @param rowRedirection      the {@link RowRedirection} to use for dense output sources
     * @param control             defines how to handle {@code null} input values.
     * @param timestampColumnName the name of the column containing timestamps for time-based calcuations
     * @param windowScaleUnits      the smoothing window for the EMA. If no {@code timestampColumnName} is provided, this is measured in ticks, otherwise it is measured in nanoseconds
     * @param valueSource         a reference to the input column source for this operation
     */
    public ShortEmStdOperator(@NotNull final MatchPair pair,
                             @NotNull final String[] affectingColumns,
                             @Nullable final RowRedirection rowRedirection,
                             @NotNull final OperationControl control,
                             @Nullable final String timestampColumnName,
                             final long windowScaleUnits,
                             final ColumnSource<?> valueSource,
                             final boolean sourceRefreshing
                             // region extra-constructor-args
                             // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, control, timestampColumnName, windowScaleUnits, sourceRefreshing);
        this.valueSource = valueSource;
        // region constructor
        // endregion constructor
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int chunkSize) {
        return new Context(chunkSize);
    }
}
