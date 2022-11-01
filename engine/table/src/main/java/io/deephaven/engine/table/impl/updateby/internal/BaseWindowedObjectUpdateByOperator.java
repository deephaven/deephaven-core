/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit BaseWindowedCharUpdateByOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedObjectChunk;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByWindowedOperator;
import io.deephaven.engine.table.impl.sources.*;
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

        protected Context(final int chunkSize) {
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
            curVal = null;
            nullCount = 0;
        }
    }

    public BaseWindowedObjectUpdateByOperator(@NotNull final MatchPair pair,
                                            @NotNull final String[] affectingColumns,
                                            @NotNull final OperationControl control,
                                            @Nullable final String timestampColumnName,
                                            final long reverseTimeScaleUnits,
                                            final long forwardTimeScaleUnits,
                                            @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                            // region extra-constructor-args
                                      , final Class<T> colType
                                            // endregion extra-constructor-args
                                    ) {
        super(pair, affectingColumns, control, timestampColumnName, reverseTimeScaleUnits, forwardTimeScaleUnits, redirContext);
        if(this.redirContext.isRedirected()) {
            // region create-dense
            this.maybeInnerSource = new ObjectArraySource(colType);
            // endregion create-dense
            this.outputSource = new WritableRedirectedColumnSource(this.redirContext.getRowRedirection(), maybeInnerSource, 0);
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
    public void initializeUpdate(@NotNull UpdateContext context) {
        // If we're redirected we have to make sure we tell the output source it's actual size, or we're going
        // to have a bad time.  This is not necessary for non-redirections since the SparseArraySources do not
        // need to do anything with capacity.
        if(redirContext.isRedirected()) {
            // The redirection index does not use the 0th index for some reason.
            outputSource.ensureCapacity(redirContext.requiredCapacity());
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
    public void applyOutputShift(@NotNull final RowSet subIndexToShift, final long delta) {
        ((ObjectSparseArraySource)outputSource).shift(subIndexToShift, delta);
    }
    // endregion Shifts

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
