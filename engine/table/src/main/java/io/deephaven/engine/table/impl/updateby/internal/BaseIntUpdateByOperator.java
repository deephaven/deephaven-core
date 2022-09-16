/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit BaseCharUpdateByOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedIntChunk;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByCumulativeOperator;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.engine.table.impl.util.SizedSafeCloseable;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

public abstract class BaseIntUpdateByOperator extends UpdateByCumulativeOperator {
    protected final WritableColumnSource<Integer> outputSource;
    protected final WritableColumnSource<Integer> maybeInnerSource;

    protected final MatchPair pair;
    protected final String[] affectingColumns;

    private UpdateBy.UpdateByRedirectionContext redirContext;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends UpdateCumulativeContext {
        public final SizedSafeCloseable<ChunkSink.FillFromContext> fillContext;
        public final SizedIntChunk<Values> outputValues;

        public int curVal = QueryConstants.NULL_INT;

        protected Context(final int chunkSize, final LongSegmentedSortedArray timestampSsa) {
            this.fillContext = new SizedSafeCloseable<>(outputSource::makeFillFromContext);
            this.fillContext.ensureCapacity(chunkSize);
            this.outputValues = new SizedIntChunk<>(chunkSize);
        }

        @Override
        public void close() {
            super.close();

            outputValues.close();
            fillContext.close();
        }
    }

    /**
     * Construct a base operator for operations that produce int outputs.
     *
     * @param pair             the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns a list of all columns (including the input column from the pair) that affects the result
     *                         of this operator.
     * @param redirContext the {@link UpdateBy.UpdateByRedirectionContext} for the overall update
     */
    public BaseIntUpdateByOperator(@NotNull final MatchPair pair,
                                    @NotNull final String[] affectingColumns,
                                    @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                    // region extra-constructor-args
                                    // endregion extra-constructor-args
                                    ) {
        this.pair = pair;
        this.affectingColumns = affectingColumns;
        this.redirContext = redirContext;
        if(this.redirContext.isRedirected()) {
            // region create-dense
            this.maybeInnerSource = new IntegerArraySource();
            // endregion create-dense
            this.outputSource = new WritableRedirectedColumnSource(this.redirContext.getRowRedirection(), maybeInnerSource, 0);
        } else {
            this.maybeInnerSource = null;
            // region create-sparse
            this.outputSource = new IntegerSparseArraySource();
            // endregion create-sparse
        }

        // region constructor
        // endregion constructor
    }

    // region extra-methods
    // endregion extra-methods

    @Override
    public void setChunkSize(@NotNull final UpdateContext context, final int chunkSize) {
        ((Context)context).outputValues.ensureCapacity(chunkSize);
        ((Context)context).fillContext.ensureCapacity(chunkSize);
    }

    @NotNull
    @Override
    public String getInputColumnName() {
        return pair.rightColumn;
    }

    @NotNull
    @Override
    public String[] getAffectingColumnNames() {
        return affectingColumns;
    }

    @NotNull
    @Override
    public String[] getOutputColumnNames() {
        return new String[] { pair.leftColumn };
    }

    @NotNull
    @Override
    public Map<String, ColumnSource<?>> getOutputColumns() {
        return Collections.singletonMap(pair.leftColumn, outputSource);
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(final int chunkSize, final LongSegmentedSortedArray timestampSsa) {
        return new Context(chunkSize, timestampSsa);
    }

    @Override
    public void startTrackingPrev() {
        outputSource.startTrackingPrevValues();
        if (redirContext.isRedirected()) {
            maybeInnerSource.startTrackingPrevValues();
        }
    }

    /**
     * Perform the processing for a chunk of values by the operator.
     *
     * @param ctx the context object
     * @param inputKeys the input keys for the chunk
     * @param workingChunk the chunk of values
     */
    protected abstract void doProcessChunk(@NotNull final Context ctx,
                                           @NotNull final RowSequence inputKeys,
                                           @NotNull final Chunk<Values> workingChunk);

    // endregion

    // region Shifts

    @Override
    public void applyOutputShift(@NotNull final RowSet subIndexToShift, final long delta) {
        ((IntegerSparseArraySource)outputSource).shift(subIndexToShift, delta);
    }

    // endregion Shifts

    // region Reprocessing

    public void resetForProcess(@NotNull final UpdateContext context,
                                @NotNull final RowSet sourceIndex,
                                long firstUnmodifiedKey) {

        // If we're redirected we have to make sure we tell the output source it's actual size, or we're going
        // to have a bad time.  This is not necessary for non-redirections since the SparseArraySources do not
        // need to do anything with capacity.
        if(redirContext.isRedirected()) {
            // The redirection index does not use the 0th index for some reason.
            outputSource.ensureCapacity(redirContext.requiredCapacity());
        }

        final Context ctx = (Context) context;
        ctx.curVal = firstUnmodifiedKey == NULL_ROW_KEY ? QueryConstants.NULL_INT : outputSource.getInt(firstUnmodifiedKey);
    }

    @Override
    public void processChunk(@NotNull final UpdateContext updateContext,
                             @NotNull final RowSequence inputKeys,
                             @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                             @Nullable final LongChunk<OrderedRowKeys> posChunk,
                             @NotNull final Chunk<Values> valuesChunk,
                             @NotNull final RowSet postUpdateSourceIndex) {
        final Context ctx = (Context) updateContext;
        doProcessChunk(ctx, inputKeys, valuesChunk);
        ctx.getModifiedBuilder().appendRowSequence(inputKeys);
    }

    // endregion
}
