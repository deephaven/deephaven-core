/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit BaseCharUpdateByOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.util.QueryConstants;
import io.deephaven.engine.table.impl.sources.ByteArraySource;
import io.deephaven.engine.table.impl.sources.ByteSparseArraySource;
import io.deephaven.engine.table.WritableColumnSource;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByCumulativeOperator;
import io.deephaven.engine.table.impl.sources.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.*;

public abstract class BaseByteUpdateByOperator extends UpdateByCumulativeOperator {
    protected final WritableColumnSource<Byte> outputSource;
    protected final WritableColumnSource<Byte> maybeInnerSource;

    // region extra-fields
    final byte nullValue;
    // endregion extra-fields

    protected abstract class Context extends UpdateByCumulativeOperator.Context {
        public final ChunkSink.FillFromContext outputFillContext;
        public final WritableByteChunk<Values> outputValues;

        public byte curVal = NULL_BYTE;

        protected Context(final int chunkSize) {
            super();
            this.outputFillContext = outputSource.makeFillFromContext(chunkSize);
            this.outputValues = WritableByteChunk.makeWritableChunk(chunkSize);
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
            curVal = NULL_BYTE;
        }
    }

    /**
     * Construct a base operator for operations that produce byte outputs.
     *
     * @param pair             the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns a list of all columns (including the input column from the pair) that affects the result
     *                         of this operator.
     * @param redirHelper the {@link UpdateBy.UpdateByRedirectionHelper} for the overall update
     */
    public BaseByteUpdateByOperator(@NotNull final MatchPair pair,
                                    @NotNull final String[] affectingColumns,
                                    @NotNull final UpdateBy.UpdateByRedirectionHelper redirHelper
                                    // region extra-constructor-args
                                    // endregion extra-constructor-args
    ) {
        this(pair, affectingColumns, redirHelper, null, 0);
    }

    /**
     * Construct a base operator for operations that produce byte outputs.
     *
     * @param pair             the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns a list of all columns (including the input column from the pair) that affects the result
     *                         of this operator.
     * @param redirHelper the {@link UpdateBy.UpdateByRedirectionHelper} for the overall update
     * @param timestampColumnName an optional timestamp column. If this is null, it will be assumed time is measured in
     *        integer ticks.
     * @param timeScaleUnits the smoothing window for the EMA. If no {@code timestampColumnName} is provided, this is
     *        measured in ticks, otherwise it is measured in nanoseconds.
     */
    public BaseByteUpdateByOperator(@NotNull final MatchPair pair,
                                    @NotNull final String[] affectingColumns,
                                    @NotNull final UpdateBy.UpdateByRedirectionHelper redirHelper,
                                    @Nullable final String timestampColumnName,
                                    final long timeScaleUnits
                                    // region extra-constructor-args
                                    // endregion extra-constructor-args
                                    ) {
        super(pair, affectingColumns, redirHelper, timestampColumnName, timeScaleUnits);
        if(this.redirHelper.isRedirected()) {
            // region create-dense
            this.maybeInnerSource = makeDenseSource();
            // endregion create-dense
            this.outputSource = new WritableRedirectedColumnSource(this.redirHelper.getRowRedirection(), maybeInnerSource, 0);
        } else {
            this.maybeInnerSource = null;
            // region create-sparse
            this.outputSource = makeSparseSource();
            // endregion create-sparse
        }

        // region constructor
        this.nullValue = getNullValue();
        // endregion constructor
    }


    // region extra-methods
    protected byte getNullValue() {
        return QueryConstants.NULL_BYTE;
    }

    // region extra-methods
    protected WritableColumnSource<Byte> makeSparseSource() {
        return new ByteSparseArraySource();
    }

    protected WritableColumnSource<Byte> makeDenseSource() {
        return new ByteArraySource();
    }
    // endregion extra-methods

    @Override
    public void initializeUpdate(@NotNull UpdateContext context, long firstUnmodifiedKey, long firstUnmodifiedTimestamp) {
        Context ctx = (Context) context;
        if (firstUnmodifiedKey != NULL_ROW_KEY) {
            ctx.curVal = outputSource.getByte(firstUnmodifiedKey);
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
        if (outputSource instanceof BooleanSparseArraySource.ReinterpretedAsByte) {
            ((BooleanSparseArraySource.ReinterpretedAsByte)outputSource).shift(subIndexToShift, delta);
        } else {
            ((ByteSparseArraySource)outputSource).shift(subIndexToShift, delta);
        }
    }
    // endregion Shifts

    @Override
    public void prepareForParallelPopulation(final RowSet changedRows) {
        if (redirHelper.isRedirected()) {
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
}
