package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedFloatChunk;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByWindowedOperator;
import io.deephaven.engine.table.impl.sources.FloatArraySource;
import io.deephaven.engine.table.impl.sources.FloatSparseArraySource;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

public abstract class BaseWindowedFloatUpdateByOperator extends UpdateByWindowedOperator {
    protected final WritableColumnSource<Float> outputSource;
    protected final WritableColumnSource<Float> maybeInnerSource;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends UpdateWindowedContext {
        public final ChunkSink.FillFromContext fillContext;
        public final SizedFloatChunk<Values> outputValues;

        protected Context(final int chunkSize) {
            this.fillContext = outputSource.makeFillFromContext(chunkSize);
            this.outputValues = new SizedFloatChunk<>(chunkSize);
        }

        public void storeWorkingChunk(@NotNull final Chunk<Values> valuesChunk) {}

        @Override
        public void close() {
            outputValues.close();
            fillContext.close();
        }
    }

    public BaseWindowedFloatUpdateByOperator(@NotNull final MatchPair pair,
                                            @NotNull final String[] affectingColumns,
                                            @NotNull final OperationControl control,
                                            @Nullable final String timestampColumnName,
                                            final long reverseTimeScaleUnits,
                                            final long forwardTimeScaleUnits,
                                            @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                            // region extra-constructor-args
                                            // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, control, timestampColumnName, reverseTimeScaleUnits, forwardTimeScaleUnits, redirContext);
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

    //*** for floating point operators, we want a computed result */
    public abstract float result(UpdateContext context);

    // region extra-methods
    // endregion extra-methods

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize, ColumnSource<?> inputSource) {
        return new Context(chunkSize);
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
        ((FloatSparseArraySource)outputSource).shift(subIndexToShift, delta);
    }

    // endregion Shifts

    // region Processing

    @Override
    public void processChunk(@NotNull final UpdateContext updateContext,
                             @NotNull final RowSequence inputKeys,
                             @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                             @Nullable final LongChunk<OrderedRowKeys> posChunk,
                             @Nullable final Chunk<Values> valuesChunk,
                             @Nullable final LongChunk<? extends Values> timestampValuesChunk) {
        final Context ctx = (Context) updateContext;
        ctx.storeWorkingChunk(valuesChunk);
        for (int ii = 0; ii < valuesChunk.size(); ii++) {
            push(ctx, keyChunk == null ? NULL_ROW_KEY : keyChunk.get(ii), ii);
            ctx.outputValues.get().set(ii, result(ctx));
        }
        outputSource.fillFromChunk(ctx.fillContext, ctx.outputValues.get(), inputKeys);
    }

    // endregion
}
