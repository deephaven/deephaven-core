/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit BaseCharUpdateByOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.engine.table.impl.util.ChunkUtils;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.updateby.UpdateByCumulativeOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.*;

public abstract class BaseObjectUpdateByOperator<T> extends UpdateByCumulativeOperator {
    protected final WritableColumnSource<T> outputSource;
    protected final WritableColumnSource<T> maybeInnerSource;

    // region extra-fields
    private final Class<T> colType;
    // endregion extra-fields

    protected abstract class Context extends UpdateByCumulativeOperator.Context {
        public final ChunkSink.FillFromContext outputFillContext;
        public final WritableObjectChunk<T, Values> outputValues;

        public T curVal = null;

        protected Context(final int chunkSize) {
            super();
            this.outputFillContext = outputSource.makeFillFromContext(chunkSize);
            this.outputValues = WritableObjectChunk.makeWritableChunk(chunkSize);
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
                push(NULL_ROW_KEY, ii, 1);
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
            curVal = null;
        }
    }

    /**
     * Construct a base operator for operations that produce Object outputs.
     *
     * @param pair             the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns a list of all columns (including the input column from the pair) that affects the result
     *                         of this operator.
     * @param rowRedirection the {@link RowRedirection} for the output column
     */
    public BaseObjectUpdateByOperator(@NotNull final MatchPair pair,
                                    @NotNull final String[] affectingColumns,
                                    @Nullable final RowRedirection rowRedirection
                                    // region extra-constructor-args
                                      , final Class<T> colType
                                    // endregion extra-constructor-args
    ) {
        this(pair, affectingColumns, rowRedirection, null, 0, colType);
    }

    /**
     * Construct a base operator for operations that produce Object outputs.
     *
     * @param pair             the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns a list of all columns (including the input column from the pair) that affects the result
     *                         of this operator.
     * @param rowRedirection the {@link RowRedirection} for the output column
     * @param timestampColumnName an optional timestamp column. If this is null, it will be assumed time is measured in
     *        integer ticks.
     * @param timeScaleUnits the smoothing window for the EMA. If no {@code timestampColumnName} is provided, this is
     *        measured in ticks, otherwise it is measured in nanoseconds.
     */
    public BaseObjectUpdateByOperator(@NotNull final MatchPair pair,
                                    @NotNull final String[] affectingColumns,
                                    @Nullable final RowRedirection rowRedirection,
                                    @Nullable final String timestampColumnName,
                                    final long timeScaleUnits
                                    // region extra-constructor-args
                                      , final Class<T> colType
                                    // endregion extra-constructor-args
                                    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, timeScaleUnits);
        if(rowRedirection != null) {
            // region create-dense
            this.maybeInnerSource = new ObjectArraySource(colType);
            // endregion create-dense
            this.outputSource = new WritableRedirectedColumnSource<>(rowRedirection, maybeInnerSource, 0);
        } else {
            this.maybeInnerSource = null;
            // region create-sparse
            this.outputSource = new ObjectSparseArraySource<>(colType);
            // endregion create-sparse
        }

        // region constructor
        this.colType = colType;
        // endregion constructor
    }


    // region extra-methods
    // endregion extra-methods

    @Override
    public void initializeUpdate(@NotNull UpdateContext context, long firstUnmodifiedKey, long firstUnmodifiedTimestamp) {
        Context ctx = (Context) context;
        if (firstUnmodifiedKey != NULL_ROW_KEY) {
            ctx.curVal = outputSource.get(firstUnmodifiedKey);
        } else {
            ctx.reset();
        }
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
        ((ObjectSparseArraySource)outputSource).shift(subRowSetToShift, delta);
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
        // if we are redirected, clear the inner source
        if (rowRedirection != null) {
            ChunkUtils.fillWithNullValue(maybeInnerSource, toClear);
        } else {
            ChunkUtils.fillWithNullValue(outputSource, toClear);
        }
    }
    // endregion clear-output
}
