/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit BaseWindowedFloatUpdateByOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.sources.DoubleSparseArraySource;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.engine.table.impl.updateby.UpdateByWindowedOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.*;

public abstract class BaseWindowedDoubleUpdateByOperator extends UpdateByWindowedOperator {
    protected final WritableColumnSource<Double> outputSource;
    protected final WritableColumnSource<Double> maybeInnerSource;

    public double curVal = NULL_DOUBLE;

    // region extra-fields
    // endregion extra-fields

    protected abstract class Context extends UpdateByWindowedOperator.Context {
        public final ChunkSink.FillFromContext outputFillContext;
        public final WritableDoubleChunk<Values> outputValues;

        protected Context(final int chunkSize) {
            this.outputFillContext = outputSource.makeFillFromContext(chunkSize);
            this.outputValues = WritableDoubleChunk.makeWritableChunk(chunkSize);
        }

        @Override
        public void accumulate(RowSequence inputKeys,
                               Chunk<? extends Values>[] influencerValueChunkArr,
                               IntChunk<? extends Values> pushChunk,
                               IntChunk<? extends Values> popChunk,
                               int len) {

            setValuesChunk(influencerValueChunkArr[0]);
            int pushIndex = 0;

            // chunk processing
            for (int ii = 0; ii < len; ii++) {
                final int pushCount = pushChunk.get(ii);
                final int popCount = popChunk.get(ii);

                if (pushCount == NULL_INT) {
                    writeNullToOutputChunk(ii);
                    continue;
                }

                // pop for this row
                if (popCount > 0) {
                    pop(popCount);
                }

                // push for this row
                if (pushCount > 0) {
                    push(NULL_ROW_KEY, pushIndex, pushCount);
                    pushIndex += pushCount;
                }

                // write the results to the output chunk
                writeToOutputChunk(ii);
            }

            // chunk output to column
            writeToOutputColumn(inputKeys);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {}

        @Override
        public void setTimestampChunk(@NotNull final LongChunk<? extends Values> valuesChunk) {}

        @Override
        public void writeToOutputChunk(int outIdx) {
            outputValues.set(outIdx, curVal);
        }

        void writeNullToOutputChunk(int outIdx) {
            outputValues.set(outIdx, NULL_DOUBLE);
        }

        @Override
        public void writeToOutputColumn(@NotNull final RowSequence inputKeys) {
            outputSource.fillFromChunk(outputFillContext, outputValues, inputKeys);
        }

        @Override
        public void close() {
            super.close();
            outputValues.close();
            outputFillContext.close();
        }

        @Override
        public void reset() {
            curVal = NULL_DOUBLE;
            nullCount = 0;
        }
    }

    public BaseWindowedDoubleUpdateByOperator(@NotNull final MatchPair pair,
                                            @NotNull final String[] affectingColumns,
                                            @NotNull final OperationControl control,
                                            @Nullable final String timestampColumnName,
                                            final long reverseTimeScaleUnits,
                                            final long forwardTimeScaleUnits,
                                            @Nullable final RowRedirection rowRedirection
                                            // region extra-constructor-args
                                            // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, control, timestampColumnName, reverseTimeScaleUnits, forwardTimeScaleUnits, rowRedirection);
        if(rowRedirection != null) {
            // region create-dense
            this.maybeInnerSource = new DoubleArraySource();
            // endregion create-dense
            this.outputSource = new WritableRedirectedColumnSource<>(rowRedirection, maybeInnerSource, 0);
        } else {
            this.maybeInnerSource = null;
            // region create-sparse
            this.outputSource = new DoubleSparseArraySource();
            // endregion create-sparse
        }

        // region constructor
        // endregion constructor
    }

    // region extra-methods
    // endregion extra-methods

    @Override
    public void initializeUpdate(@NotNull UpdateContext context) {
    }

    @Override
    public void startTrackingPrev() {
        outputSource.startTrackingPrevValues();
        if (rowRedirection != null) {
            assert maybeInnerSource != null;
            maybeInnerSource.startTrackingPrevValues();
        }
    }

    // region Shifts
    @Override
    public void applyOutputShift(@NotNull final RowSet subRowSetToShift, final long delta) {
        ((DoubleSparseArraySource)outputSource).shift(subRowSetToShift, delta);
    }
    // endregion Shifts

    @Override
    public void prepareForParallelPopulation(final RowSet changedRows) {
        if (rowRedirection != null) {
            assert maybeInnerSource != null;
            ((WritableSourceWithPrepareForParallelPopulation) maybeInnerSource).prepareForParallelPopulation(changedRows);
        } else {
            ((WritableSourceWithPrepareForParallelPopulation) outputSource).prepareForParallelPopulation(changedRows);
        }
    }

    @NotNull
    @Override
    public Map<String, ColumnSource<?>> getOutputColumns() {
        return Collections.singletonMap(pair.leftColumn, outputSource);
    }

    // region clear-output
    @Override
    public void clearOutputRows(final RowSet toClear) {
        // NOP for primitive types
    }
    // endregion clear-output
}
