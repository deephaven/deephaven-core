/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit BaseCharUpdateByOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByCumulativeOperator;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Map;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.*;

public abstract class BaseLongUpdateByOperator extends UpdateByCumulativeOperator {
    protected final WritableColumnSource<Long> outputSource;
    protected final WritableColumnSource<Long> maybeInnerSource;

    // region extra-fields
    // endregion extra-fields

    protected abstract class Context extends UpdateByCumulativeOperator.Context {
        public final ChunkSink.FillFromContext outputFillContext;
        public final WritableLongChunk<Values> outputValues;

        public long curVal = NULL_LONG;

        protected Context(final int chunkSize) {
            super(chunkSize);
            this.outputFillContext = outputSource.makeFillFromContext(chunkSize);
            this.outputValues = WritableLongChunk.makeWritableChunk(chunkSize);
        }

        @Override
        public void accumulate(RowSequence inputKeys,
                               Chunk<? extends Values>[] valueChunkArr,
                               LongChunk<? extends Values> tsChunk,
                               int len) {

            setValuesChunk(valueChunkArr[0]);
            setTimestampChunk(tsChunk);

            // chunk processing
            for (int ii = 0; ii < len; ii++) {
                push(NULL_ROW_KEY, ii);
                writeToOutputChunk(ii);
            }

            // chunk output to column
            writeToOutputColumn(inputKeys);
        }

        @Override
        public void close() {
            super.close();
            outputValues.close();
            outputFillContext.close();
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {}

        @Override
        public void setTimestampChunk(@NotNull final LongChunk<? extends Values> valuesChunk) {}

        @Override
        public void writeToOutputChunk(int outIdx) {
            outputValues.set(outIdx, curVal);
        }

        @Override
        public void writeToOutputColumn(@NotNull final RowSequence inputKeys) {
            outputSource.fillFromChunk(outputFillContext, outputValues, inputKeys);
        }

        @Override
        public void reset() {
            curVal = NULL_LONG;
        }
    }

    /**
     * Construct a base operator for operations that produce long outputs.
     *
     * @param pair             the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns a list of all columns (including the input column from the pair) that affects the result
     *                         of this operator.
     * @param redirHelper the {@link UpdateBy.UpdateByRedirectionHelper} for the overall update
     */
    public BaseLongUpdateByOperator(@NotNull final MatchPair pair,
                                    @NotNull final String[] affectingColumns,
                                    @NotNull final UpdateBy.UpdateByRedirectionHelper redirHelper
                                    // region extra-constructor-args
                                    // endregion extra-constructor-args
                                    ) {
        super(pair, affectingColumns, redirHelper);
        if(this.redirHelper.isRedirected()) {
            // region create-dense
            this.maybeInnerSource = new LongArraySource();
            // endregion create-dense
            this.outputSource = new WritableRedirectedColumnSource(this.redirHelper.getRowRedirection(), maybeInnerSource, 0);
        } else {
            this.maybeInnerSource = null;
            // region create-sparse
            this.outputSource = new LongSparseArraySource();
            // endregion create-sparse
        }

        // region constructor
        // endregion constructor
    }


    // region extra-methods
    // endregion extra-methods

    @Override
    public void initializeUpdate(@NotNull UpdateContext context, long firstUnmodifiedKey, long firstUnmodifiedTimestamp) {
        Context ctx = (Context) context;
        if (firstUnmodifiedKey != NULL_ROW_KEY) {
            ctx.curVal = outputSource.getLong(firstUnmodifiedKey);
        } else {
            ctx.reset();
        }
    }

    @Override
    public void startTrackingPrev() {
        outputSource.startTrackingPrevValues();
        if (redirHelper.isRedirected()) {
            maybeInnerSource.startTrackingPrevValues();
        }
    }

    // region Shifts
    @Override
    public void applyOutputShift(@NotNull final RowSet subIndexToShift, final long delta) {
        ((LongSparseArraySource)outputSource).shift(subIndexToShift, delta);
    }
    // endregion Shifts

    @Override
    public void prepareForParallelPopulation(final RowSet added) {
        if (redirHelper.isRedirected()) {
            ((WritableSourceWithPrepareForParallelPopulation) maybeInnerSource).prepareForParallelPopulation(added);
        } else {
            ((WritableSourceWithPrepareForParallelPopulation) outputSource).prepareForParallelPopulation(added);
        }
    }

    @NotNull
    @Override
    public Map<String, ColumnSource<?>> getOutputColumns() {
        return Collections.singletonMap(pair.leftColumn, outputSource);
    }
}
