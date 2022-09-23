package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedCharChunk;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
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

public abstract class BaseCharUpdateByOperator extends UpdateByCumulativeOperator {
    protected final WritableColumnSource<Character> outputSource;
    protected final WritableColumnSource<Character> maybeInnerSource;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends UpdateByCumulativeOperator.Context {
        public final ChunkSink.FillFromContext fillContext;
        public final WritableCharChunk<Values> outputValues;

        public char curVal = NULL_CHAR;

        protected Context(final int chunkSize) {
            super(chunkSize);
            this.fillContext = outputSource.makeFillFromContext(chunkSize);
            this.outputValues = WritableCharChunk.makeWritableChunk(chunkSize);
        }

        public void storeValuesChunk(@NotNull final Chunk<Values> valuesChunk) {}

        @Override
        public void close() {
            super.close();
            outputValues.close();
            fillContext.close();
        }
    }

    /**
     * Construct a base operator for operations that produce char outputs.
     *
     * @param pair             the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns a list of all columns (including the input column from the pair) that affects the result
     *                         of this operator.
     * @param redirContext the {@link UpdateBy.UpdateByRedirectionContext} for the overall update
     */
    public BaseCharUpdateByOperator(@NotNull final MatchPair pair,
                                    @NotNull final String[] affectingColumns,
                                    @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                    // region extra-constructor-args
                                    // endregion extra-constructor-args
                                    ) {
        super(pair, affectingColumns, redirContext);
        if(this.redirContext.isRedirected()) {
            // region create-dense
            this.maybeInnerSource = new CharacterArraySource();
            // endregion create-dense
            this.outputSource = new WritableRedirectedColumnSource(this.redirContext.getRowRedirection(), maybeInnerSource, 0);
        } else {
            this.maybeInnerSource = null;
            // region create-sparse
            this.outputSource = new CharacterSparseArraySource();
            // endregion create-sparse
        }

        // region constructor
        // endregion constructor
    }

    @Override
    public void reset(UpdateContext context) {
        final Context ctx = (Context)context;
        ctx.curVal = NULL_CHAR;
    }

    // region extra-methods
    // endregion extra-methods

    @Override
    public void initializeUpdate(@NotNull UpdateContext context, long firstUnmodifiedKey, long firstUnmodifiedTimestamp) {
        Context ctx = (Context) context;
        if (firstUnmodifiedKey != NULL_ROW_KEY) {
            ctx.curVal = outputSource.getChar(firstUnmodifiedKey);
        } else {
            reset(ctx);
        }

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
        ((CharacterSparseArraySource)outputSource).shift(subIndexToShift, delta);
    }
    // endregion Shifts

    // region Processing
    @Override
    public void processChunk(@NotNull final UpdateContext updateContext,
                             @NotNull final RowSequence inputKeys,
                             @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                             @Nullable final LongChunk<OrderedRowKeys> posChunk,
                             @Nullable final Chunk<Values> valuesChunk,
                             @Nullable final LongChunk<Values> timestampValuesChunk) {
        Assert.neqNull(valuesChunk, "valuesChunk must not be null for a cumulative operator");
        final Context ctx = (Context) updateContext;
        ctx.storeValuesChunk(valuesChunk);
        for (int ii = 0; ii < valuesChunk.size(); ii++) {
            push(ctx, keyChunk == null ? NULL_ROW_KEY : keyChunk.get(ii), ii);
            ctx.outputValues.set(ii, ctx.curVal);
        }
        outputSource.fillFromChunk(ctx.fillContext, ctx.outputValues, inputKeys);
    }
    // endregion

    @NotNull
    @Override
    public Map<String, ColumnSource<?>> getOutputColumns() {
        return Collections.singletonMap(pair.leftColumn, outputSource);
    }
}
