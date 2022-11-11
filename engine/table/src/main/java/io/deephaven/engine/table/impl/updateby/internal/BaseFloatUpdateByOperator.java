package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByCumulativeOperator;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public abstract class BaseFloatUpdateByOperator extends UpdateByCumulativeOperator {
    protected final WritableColumnSource<Float> outputSource;
    protected final WritableColumnSource<Float> maybeInnerSource;

    // region extra-fields
    // endregion extra-fields

    protected abstract class Context extends UpdateByCumulativeOperator.Context {
        public final ChunkSink.FillFromContext outputFillContext;
        public final WritableFloatChunk<Values> outputValues;

        public float curVal = NULL_FLOAT;

        protected Context(final int chunkSize) {
            super(chunkSize);
            this.outputFillContext = outputSource.makeFillFromContext(chunkSize);
            this.outputValues = WritableFloatChunk.makeWritableChunk(chunkSize);
        }

        @Override
        public void accumulate(RowSequence inputKeys,
                               Chunk<? extends Values> valueChunkArr[],
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

        @FinalDefault
        public void reset() {
            curVal = NULL_FLOAT;
        }
    }

    /**
     * Construct a base operator for operations that produce float outputs.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns a list of all columns (including the input column from the pair) that affects the result
     *                         of this operator.
     * @param redirContext the {@link UpdateBy.UpdateByRedirectionContext} for the overall update
     */
    public BaseFloatUpdateByOperator(@NotNull final MatchPair pair,
                                     @NotNull final String[] affectingColumns,
                                     @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                     // region extra-constructor-args
                                     // endregion extra-constructor-args
                                     ) {
        super(pair, affectingColumns, redirContext);
        if(this.redirContext.isRedirected()) {
            // region create-dense
            this.maybeInnerSource = new FloatArraySource();
            // endregion create-dense
            this.outputSource = new WritableRedirectedColumnSource(this.redirContext.getRowRedirection(), maybeInnerSource, 0);
        } else {
            this.maybeInnerSource = null;
            // region create-sparse
            this.outputSource = new FloatSparseArraySource();
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
            ctx.curVal = outputSource.getFloat(firstUnmodifiedKey);
        } else {
            ctx.reset();
        }
    }

    @Override
    public void startTrackingPrev() {
        outputSource.startTrackingPrevValues();
        if (redirContext.isRedirected()) {
            maybeInnerSource.startTrackingPrevValues();
        }
    }

    // region Shifts
    @Override
    public void applyOutputShift(@NotNull final RowSet subRowSetToShift, final long delta) {
        ((FloatSparseArraySource)outputSource).shift(subRowSetToShift, delta);
    }
    // endregion

    @Override
    public void prepareForParallelPopulation(final RowSet added) {
        // we don't need to do anything for redirected, that happened earlier
        if (!redirContext.isRedirected()) {
            ((SparseArrayColumnSource<?>) outputSource).prepareForParallelPopulation(added);
        }
    }

    @NotNull
    @Override
    public Map<String, ColumnSource<?>> getOutputColumns() {
        return Collections.singletonMap(pair.leftColumn, outputSource);
    }
}
