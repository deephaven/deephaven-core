/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit BaseWindowedCharUpdateByOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedLongChunk;
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

public abstract class BaseWindowedLongUpdateByOperator extends UpdateByWindowedOperator {
    protected final WritableColumnSource<Long> outputSource;
    protected final WritableColumnSource<Long> maybeInnerSource;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends UpdateWindowedContext {
        public final ChunkSink.FillFromContext fillContext;
        public final SizedLongChunk<Values> outputValues;

        public long curVal = NULL_LONG;

        protected Context(final int chunkSize) {
            this.fillContext = outputSource.makeFillFromContext(chunkSize);
            this.outputValues = new SizedLongChunk<>(chunkSize);
        }

        @Override
        public void close() {
            outputValues.close();
            fillContext.close();
        }
    }

    public BaseWindowedLongUpdateByOperator(@NotNull final MatchPair pair,
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
            this.maybeInnerSource = new LongArraySource();
            // endregion create-dense
            this.outputSource = new WritableRedirectedColumnSource(this.redirContext.getRowRedirection(), maybeInnerSource, 0);
        } else {
            this.maybeInnerSource = null;
            // region create-sparse
            this.outputSource = new LongSparseArraySource();
            // endregion create-sparse
        }

        // region constructor
        // endregion constructor
    }

    @Override
    public void reset(UpdateContext context) {
        final Context ctx = (Context)context;
        ctx.curVal = NULL_LONG;
        ctx.nullCount = 0;
    }

    // region extra-methods
    // endregion extra-methods


    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize) {
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
        ((LongSparseArraySource)outputSource).shift(subIndexToShift, delta);
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
        final Context ctx = (Context) updateContext;
        for (int ii = 0; ii < valuesChunk.size(); ii++) {
            push(ctx, keyChunk == null ? NULL_ROW_KEY : keyChunk.get(ii), ii);
            ctx.outputValues.get().set(ii, ctx.curVal);
        }
        outputSource.fillFromChunk(ctx.fillContext, ctx.outputValues.get(), inputKeys);
    }
    // endregion

    @NotNull
    @Override
    public Map<String, ColumnSource<?>> getOutputColumns() {
        return Collections.singletonMap(pair.leftColumn, outputSource);
    }
}
