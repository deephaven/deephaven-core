/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit BaseCharUpdateByOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedIntChunk;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.impl.util.SizedSafeCloseable;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

public abstract class BaseIntUpdateByOperator implements UpdateByOperator {
    protected final WritableColumnSource<Integer> outputSource;
    protected final WritableColumnSource<Integer> maybeInnerSource;
    protected final MatchPair pair;
    protected final String[] affectingColumns;
    protected final boolean isRedirected;

    protected IntegerArraySource bucketLastVal;

    /** These are only used in grouped operations */
    protected int singletonVal;
    protected long singletonGroup = QueryConstants.NULL_LONG;

    protected boolean initialized = false;

    // region extra-fields
    // endregion extra-fields

    protected class Context implements UpdateContext {
        public final SizedSafeCloseable<ChunkSink.FillFromContext> fillContext;
        public final SizedIntChunk<Values> outputValues;
        public boolean canProcessDirectly;
        public UpdateBy.UpdateType currentUpdateType;

        public RowSetBuilderSequential modifiedBuilder;
        public RowSet newModified;

        public int curVal = QueryConstants.NULL_INT;

        protected Context(final int chunkSize) {
            this.fillContext = new SizedSafeCloseable<>(outputSource::makeFillFromContext);
            this.fillContext.ensureCapacity(chunkSize);
            this.outputValues = new SizedIntChunk<>(chunkSize);
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
     * Construct a base operator for operations that produce int outputs.
     *
     * @param pair             the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns a list of all columns (including the input column from the pair) that affects the result
     *                         of this operator.
     * @param rowRedirection the {@link RowRedirection} if one is used
     */
    public BaseIntUpdateByOperator(@NotNull final MatchPair pair,
                                    @NotNull final String[] affectingColumns,
                                    @Nullable final RowRedirection rowRedirection
                                    // region extra-constructor-args
                                    // endregion extra-constructor-args
                                    ) {
        this.pair = pair;
        this.affectingColumns = affectingColumns;
        this.isRedirected = rowRedirection != null;
        if(rowRedirection != null) {
            // region create-dense
            this.maybeInnerSource = new IntegerArraySource();
            // endregion create-dense
            this.outputSource = WritableRedirectedColumnSource.maybeRedirect(rowRedirection, maybeInnerSource, 0);
        } else {
            this.maybeInnerSource = null;
            // region create-sparse
            this.outputSource = new IntegerSparseArraySource();
            // endregion create-sparse
        }

        // region constructor
        // endregion constructor

        this.singletonVal = QueryConstants.NULL_INT;
    }

    // region extra-methods
    // endregion extra-methods

    @Override
    public void setChunkSize(@NotNull final UpdateContext context, final int chunkSize) {
        ((Context)context).outputValues.ensureCapacity(chunkSize);
        ((Context)context).fillContext.ensureCapacity(chunkSize);
    }

    @Override
    public void setBucketCapacity(final int capacity) {
        bucketLastVal.ensureCapacity(capacity);
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
    public UpdateContext makeUpdateContext(final int chunkSize) {
        return new Context(chunkSize);
    }

    @Override
    public void initializeForUpdate(@NotNull final UpdateContext context,
                                    @NotNull final TableUpdate upstream,
                                    @NotNull final RowSet resultSourceIndex,
                                    final boolean usingBuckets,
                                    final boolean isUpstreamAppendOnly) {
        final Context ctx = (Context) context;
        if(!initialized) {
            initialized = true;
            if(usingBuckets) {
                // region create-bucket
                this.bucketLastVal = new IntegerArraySource();
                // endregion create-bucket
            }
        }

        // If we're redirected we have to make sure we tell the output source it's actual size, or we're going
        // to have a bad time.  This is not necessary for non-redirections since the SparseArraySources do not
        // need to do anything with capacity.
        if(isRedirected) {
            // The redirection index does not use the 0th index for some reason.
            outputSource.ensureCapacity(resultSourceIndex.size() + 1);
        }

        if(!usingBuckets) {
            // If we aren't bucketing, we'll just remember the appendyness.
            ctx.canProcessDirectly = isUpstreamAppendOnly;
        }
    }

    @Override
    public void initializeFor(@NotNull final UpdateContext updateContext,
                              @NotNull final RowSet updateIndex,
                              @NotNull final UpdateBy.UpdateType type) {
        ((Context)updateContext).currentUpdateType = type;
        ((Context)updateContext).curVal = QueryConstants.NULL_INT;
    }

    @Override
    public void finishFor(@NotNull final UpdateContext updateContext, @NotNull final UpdateBy.UpdateType type) {
        final Context ctx = (Context) updateContext;
        if(type == UpdateBy.UpdateType.Reprocess) {
            ctx.newModified = ctx.modifiedBuilder.build();
        }
    }

    @Override
    public boolean requiresKeys() {
        return false;
    }

    @Override
    public boolean requiresValues(@NotNull final UpdateContext context) {
        final Context ctx = (Context) context;
        // We only need to read actual values if we can process them at this moment,  so that means if we are
        // either part of an append-only update, or within the reprocess cycle
        return (ctx.currentUpdateType == UpdateBy.UpdateType.Add && ctx.canProcessDirectly) || ctx.currentUpdateType == UpdateBy.UpdateType.Reprocess;
    }

    @Override
    public void startTrackingPrev() {
        outputSource.startTrackingPrevValues();
        if(isRedirected) {
            maybeInnerSource.startTrackingPrevValues();
        }
    }

    @NotNull
    @Override
    final public RowSet getAdditionalModifications(@NotNull final UpdateContext ctx) {
        return ((Context)ctx).newModified == null ? RowSetFactory.empty() : ((Context)ctx).newModified;
    }

    @Override
    final public boolean anyModified(@NotNull final UpdateContext ctx) {
        return ((Context)ctx).newModified != null;
    }

    @Override
    public void onBucketsRemoved(@NotNull final RowSet removedBuckets) {
        if(bucketLastVal != null) {
            bucketLastVal.setNull(removedBuckets);
        } else {
            singletonVal = QueryConstants.NULL_INT;
        }
    }

    @Override
    public boolean canProcessNormalUpdate(@NotNull UpdateContext context) {
        return ((Context)context).canProcessDirectly;
    }

    // region Addition
    @Override
    public void addChunk(@NotNull final UpdateContext updateContext,
                         @NotNull final RowSequence inputKeys,
                         @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                         @NotNull final Chunk<Values> values,
                         long bucketPosition) {
        final Context ctx = (Context) updateContext;
        if (ctx.canProcessDirectly) {
            doAddChunk(ctx, inputKeys, values, bucketPosition);
        }
    }

    /**
     * Add a chunk of values to the operator.
     *
     * @param ctx the context object
     * @param inputKeys the input keys for the chunk
     * @param workingChunk the chunk of values
     * @param bucketPosition the bucket position that the values belong to.
     */
    protected abstract void doAddChunk(@NotNull final Context ctx,
                                       @NotNull final RowSequence inputKeys,
                                       @NotNull final Chunk<Values> workingChunk,
                                       final long bucketPosition);

    // endregion

    // region Shifts

    @Override
    public void applyOutputShift(@NotNull final UpdateContext context,
                                 @NotNull final RowSet subIndexToShift,
                                 final long delta) {
        ((IntegerSparseArraySource)outputSource).shift(subIndexToShift, delta);
    }

    // endregion Shifts

    // region Reprocessing

    public void resetForReprocess(@NotNull final UpdateContext context,
                                  @NotNull final RowSet sourceIndex,
                                  long firstUnmodifiedKey) {
        final Context ctx = (Context) context;
        if(!ctx.canProcessDirectly) {
            singletonVal = firstUnmodifiedKey == NULL_ROW_KEY ? QueryConstants.NULL_INT : outputSource.getInt(firstUnmodifiedKey);
        }
    }


    @Override
    public void resetForReprocess(@NotNull final UpdateContext ctx,
                                  @NotNull final RowSet bucketIndex,
                                  final long bucketPosition,
                                  final long firstUnmodifiedKey) {
        final int previousVal = firstUnmodifiedKey == NULL_ROW_KEY ? QueryConstants.NULL_INT : outputSource.getInt(firstUnmodifiedKey);
        bucketLastVal.set(bucketPosition, previousVal);
    }

    @Override
    public void reprocessChunk(@NotNull final UpdateContext updateContext,
                               @NotNull final RowSequence inputKeys,
                               @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                               @NotNull final Chunk<Values> valuesChunk,
                               @NotNull final RowSet postUpdateSourceIndex) {
        final Context ctx = (Context) updateContext;
        doAddChunk(ctx, inputKeys, valuesChunk, 0);
        ctx.getModifiedBuilder().appendRowSequence(inputKeys);
    }

    @Override
    public void reprocessChunk(@NotNull UpdateContext updateContext,
                               @NotNull final RowSequence chunkOk,
                               @NotNull final Chunk<Values> values,
                               @NotNull final LongChunk<? extends RowKeys> keyChunk,
                               @NotNull final IntChunk<RowKeys> bucketPositions,
                               @NotNull final IntChunk<ChunkPositions> runStartPositions,
                               @NotNull final IntChunk<ChunkLengths> runLengths) {
        addChunk(updateContext, values, keyChunk, bucketPositions, runStartPositions, runLengths);
        ((Context)updateContext).getModifiedBuilder().appendRowSequence(chunkOk);
    }

    // endregion

    // region No-Op Operations

    @Override
    final public void modifyChunk(@NotNull final UpdateContext updateContext,
                                  @Nullable final LongChunk<OrderedRowKeys> prevKeyChunk,
                                  @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                                  @NotNull final Chunk<Values> prevValuesChunk,
                                  @NotNull final Chunk<Values> postValuesChunk,
                                  long bucketPosition) {
    }

    @Override
    final public void removeChunk(@NotNull final UpdateContext updateContext,
                                  @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                                  @NotNull final Chunk<Values> prevValuesChunk,
                                  long bucketPosition) {
    }

    @Override
    final public void applyShift(@NotNull final UpdateContext updateContext,
                                 @NotNull final RowSet prevIndex,
                                 @NotNull final RowSetShiftData shifted) {

    }
    // endregion
}
