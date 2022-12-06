package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByWindowedOperator;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.*;

public abstract class BaseWindowedCharUpdateByOperator extends UpdateByWindowedOperator {
    protected final WritableColumnSource<Character> outputSource;
    protected final WritableColumnSource<Character> maybeInnerSource;

    // region extra-fields
    // endregion extra-fields

    protected abstract class Context extends UpdateByWindowedOperator.Context {
        public final ChunkSink.FillFromContext outputFillContext;
        public final WritableCharChunk<Values> outputValues;

        public char curVal = NULL_CHAR;

        protected Context(final int chunkSize) {
            this.outputFillContext = outputSource.makeFillFromContext(chunkSize);
            this.outputValues = WritableCharChunk.makeWritableChunk(chunkSize);
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

                // pop for this row
                for (int count = 0; count < popCount; count++) {
                    pop();
                }

                // push for this row
                for (int count = 0; count < pushCount; count++) {
                    push(NULL_ROW_KEY, pushIndex + count);
                }
                pushIndex += pushCount;

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
            curVal = NULL_CHAR;
            nullCount = 0;
        }
    }

    public BaseWindowedCharUpdateByOperator(@NotNull final MatchPair pair,
                                            @NotNull final String[] affectingColumns,
                                            @NotNull final OperationControl control,
                                            @Nullable final String timestampColumnName,
                                            final long reverseTimeScaleUnits,
                                            final long forwardTimeScaleUnits,
                                            @NotNull final UpdateBy.UpdateByRedirectionHelper redirHelper
                                            // region extra-constructor-args
                                            // endregion extra-constructor-args
                                    ) {
        super(pair, affectingColumns, control, timestampColumnName, reverseTimeScaleUnits, forwardTimeScaleUnits, redirHelper);
        if (this.redirHelper.isRedirected()) {
            // region create-dense
            this.maybeInnerSource = new CharacterArraySource();
            // endregion create-dense
            this.outputSource = new WritableRedirectedColumnSource(this.redirHelper.getRowRedirection(), maybeInnerSource, 0);
        } else {
            this.maybeInnerSource = null;
            // region create-sparse
            this.outputSource = new CharacterSparseArraySource();
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
        if (redirHelper.isRedirected()) {
            maybeInnerSource.startTrackingPrevValues();
        }
    }

    // region Shifts
    @Override
    public void applyOutputShift(@NotNull final RowSet subIndexToShift, final long delta) {
        ((CharacterSparseArraySource)outputSource).shift(subIndexToShift, delta);
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
