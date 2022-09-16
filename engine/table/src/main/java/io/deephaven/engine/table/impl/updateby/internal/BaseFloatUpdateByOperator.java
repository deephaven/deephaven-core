package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedFloatChunk;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByCumulativeOperator;
import io.deephaven.engine.table.impl.sources.FloatArraySource;
import io.deephaven.engine.table.impl.sources.FloatSparseArraySource;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.engine.table.impl.util.SizedSafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public abstract class BaseFloatUpdateByOperator extends UpdateByCumulativeOperator {
    protected final WritableColumnSource<Float> outputSource;
    protected final WritableColumnSource<Float> maybeInnerSource;

    private final MatchPair pair;
    private final String[] affectingColumns;

    private UpdateBy.UpdateByRedirectionContext redirContext;

    protected class Context extends UpdateCumulativeContext {
        public final SizedSafeCloseable<ChunkSink.FillFromContext> fillContext;
        public final SizedFloatChunk<Values> outputValues;

        public RowSetBuilderSequential modifiedBuilder;
        public RowSet newModified;

        public float curVal = NULL_FLOAT;

        public boolean filledWithPermanentValue = false;

        protected Context(final int chunkSize, final LongSegmentedSortedArray timestampSsa) {
            this.fillContext = new SizedSafeCloseable<>(outputSource::makeFillFromContext);
            this.fillContext.ensureCapacity(chunkSize);
            this.outputValues = new SizedFloatChunk<>(chunkSize);
            this.timestampSsa = timestampSsa;
        }

        public RowSetBuilderSequential getModifiedBuilder() {
            if(modifiedBuilder == null) {
                modifiedBuilder = RowSetFactory.builderSequential();
            }
            return modifiedBuilder;
        }

        @Override
        public void close() {
            outputValues.close();
            fillContext.close();
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
                                     @NotNull final UpdateBy.UpdateByRedirectionContext redirContext) {
        this.pair = pair;
        this.affectingColumns = affectingColumns;
        this.redirContext = redirContext;
        if(this.redirContext.isRedirected()) {
            this.maybeInnerSource = new FloatArraySource();
            this.outputSource = new WritableRedirectedColumnSource(this.redirContext.getRowRedirection(), maybeInnerSource, 0);
        } else {
            this.maybeInnerSource = null;
            this.outputSource = new FloatSparseArraySource();
        }
    }

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

    protected abstract void doProcessChunk(@NotNull final Context ctx,
                                       @NotNull final RowSequence inputKeys,
                                       @NotNull final Chunk<Values> workingChunk);
    // endregion

    // region Shifts

    @Override
    public void applyOutputShift(@NotNull final RowSet subRowSetToShift, final long delta) {
        ((FloatSparseArraySource)outputSource).shift(subRowSetToShift, delta);
    }

    // endregion

    // region Reprocessing

    public void resetForProcess(@NotNull final UpdateContext context,
                                @NotNull final RowSet sourceRowSet,
                                final long firstUnmodifiedKey) {
        final Context ctx = (Context) context;

        // If we're redirected we have to make sure we tell the output source it's actual size, or we're going
        // to have a bad time.  This is not necessary for non-redirections since the SparseArraySources do not
        // need to do anything with capacity.
        if(redirContext.isRedirected()) {
            outputSource.ensureCapacity(redirContext.requiredCapacity());
        }

        ctx.curVal = firstUnmodifiedKey == NULL_ROW_KEY ? NULL_FLOAT : outputSource.getFloat(firstUnmodifiedKey);
    }

    @Override
    public void processChunk(@NotNull final UpdateContext updateContext,
                             @NotNull final RowSequence inputKeys,
                             @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                             @Nullable final LongChunk<OrderedRowKeys> posChunk,
                             @NotNull final Chunk<Values> valuesChunk,
                             @NotNull final RowSet postUpdateSourceRowSet) {
        final Context ctx = (Context) updateContext;
        doProcessChunk(ctx, inputKeys, valuesChunk);
        ctx.getModifiedBuilder().appendRowSequence(inputKeys);
    }
    // endregion
}
