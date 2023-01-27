/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit BaseWindowedCharUpdateByOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.engine.table.impl.util.ChunkUtils;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.updateby.UpdateByWindowedOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.*;

public abstract class BaseWindowedObjectUpdateByOperator<T> extends UpdateByWindowedOperator {
    protected final WritableColumnSource<T> outputSource;
    protected final WritableColumnSource<T> maybeInnerSource;

    // region extra-fields
    private final Class<T> colType;
    // endregion extra-fields

    protected abstract class Context extends UpdateByWindowedOperator.Context {
        public final ChunkSink.FillFromContext outputFillContext;
        public final WritableObjectChunk<T, Values> outputValues;

        public T curVal = null;

        protected Context(final int chunkSize, final int chunkCount) {
            super(chunkCount);
            this.outputFillContext = outputSource.makeFillFromContext(chunkSize);
            this.outputValues = WritableObjectChunk.makeWritableChunk(chunkSize);
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
        public void writeToOutputChunk(int outIdx) {
            outputValues.set(outIdx, curVal);
        }

        void writeNullToOutputChunk(int outIdx) {
            outputValues.set(outIdx, null);
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
            curVal = null;
            nullCount = 0;
        }
    }

    public BaseWindowedObjectUpdateByOperator(@NotNull final MatchPair pair,
                                            @NotNull final String[] affectingColumns,
                                            @Nullable final String timestampColumnName,
                                            final long reverseWindowScaleUnits,
                                            final long forwardWindowScaleUnits,
                                            @Nullable final RowRedirection rowRedirection
                                            // region extra-constructor-args
                                      , final Class<T> colType
                                            // endregion extra-constructor-args
                                    ) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, rowRedirection);
        if (rowRedirection != null) {
            // region create-dense
            this.maybeInnerSource = new ObjectArraySource<>(colType);
            // endregion create-dense
            this.outputSource = WritableRedirectedColumnSource.maybeRedirect(rowRedirection, maybeInnerSource, 0);
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
