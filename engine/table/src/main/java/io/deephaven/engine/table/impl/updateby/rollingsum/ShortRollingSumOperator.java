package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.engine.table.impl.updateby.internal.*;
import io.deephaven.engine.table.impl.util.SizedSafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.QueryConstants.NULL_SHORT;

public class ShortRollingSumOperator extends BaseWindowedShortUpdateByOperator {

    // RollingSum will output Long values for integral types
    private final WritableColumnSource<Long> outputSource;
    private final WritableColumnSource<Long> maybeInnerSource;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseWindowedShortUpdateByOperator.Context {
        public final SizedSafeCloseable<ChunkSink.FillFromContext> fillContext;
        public final SizedLongChunk<Values> outputValues;

        public long currentVal = NULL_LONG;

        protected Context(final int chunkSize, final LongSegmentedSortedArray timestampSsa) {
            this.fillContext = new SizedSafeCloseable<>(outputSource::makeFillFromContext);
            this.fillContext.ensureCapacity(chunkSize);
            this.outputValues = new SizedLongChunk<>(chunkSize);
            this.timestampSsa = timestampSsa;
        }

        @Override
        public void close() {
            super.close();
            outputValues.close();
            fillContext.close();
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.UpdateContext makeUpdateContext(final int chunkSize, final LongSegmentedSortedArray timestampSsa) {
        return new Context(chunkSize, timestampSsa);
    }

    @Override
    public void setChunkSize(@NotNull UpdateContext context, int chunkSize) {
        final Context ctx = (Context) context;
        ctx.outputValues.ensureCapacity(chunkSize);
        ctx.fillContext.ensureCapacity(chunkSize);
    }

    public ShortRollingSumOperator(@NotNull final MatchPair pair,
                                   @NotNull final String[] affectingColumns,
                                   @NotNull final OperationControl control,
                                   @Nullable final String timestampColumnName,
                                   @Nullable final ColumnSource<?> timestampColumnSource,
                                   final long reverseTimeScaleUnits,
                                   final long forwardTimeScaleUnits,
                                   @NotNull final UpdateBy.UpdateByRedirectionContext redirContext,
                                   @NotNull final ColumnSource<Short> valueSource
                                   // region extra-constructor-args
                                   // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, control, timestampColumnName, timestampColumnSource, reverseTimeScaleUnits, forwardTimeScaleUnits, redirContext, valueSource);
        if(redirContext.isRedirected()) {
            // region create-dense
            this.maybeInnerSource = new LongArraySource();
            // endregion create-dense
            this.outputSource = new WritableRedirectedColumnSource(redirContext.getRowRedirection(), maybeInnerSource, 0);
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
    public void push(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;
        short val = ctx.candidateValuesChunk.get(pos);

        // increase the running sum
        if (val != NULL_SHORT) {
            if (ctx.currentVal == NULL_LONG) {
                ctx.currentVal = val;
            } else {
                ctx.currentVal += val;
            }
        } else {
            ctx.nullCount++;
        }
    }

    @Override
    public void pop(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;
        short val = ctx.candidateValuesChunk.get(pos);

        // reduce the running sum
        if (val != NULL_SHORT) {
            ctx.currentVal -= val;
        } else {
            ctx.nullCount--;
        }
    }

    @Override
    public void reset(UpdateContext context) {
        final Context ctx = (Context) context;
        ctx.currentVal = NULL_LONG;
    }

    @Override
    public void doProcessChunk(@NotNull final BaseWindowedShortUpdateByOperator.Context context,
                              @NotNull final RowSequence inputKeys,
                              @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                              @Nullable final LongChunk<OrderedRowKeys> posChunk,
                              @NotNull final Chunk<Values> workingChunk) {
        final Context ctx = (Context) context;

        if (timestampColumnName == null) {
            computeTicks(ctx, posChunk, inputKeys.intSize());
        } else {
            computeTime(ctx, inputKeys);
        }

        //noinspection unchecked
        outputSource.fillFromChunk(ctx.fillContext.get(), ctx.outputValues.get(), inputKeys);
    }

    private void computeTicks(@NotNull final Context ctx,
                              @Nullable final LongChunk<OrderedRowKeys> posChunk,
                              final int runLength) {

        final WritableLongChunk<Values> localOutputValues = ctx.outputValues.get();
        for (int ii = 0; ii < runLength; ii++) {
            // the output value is computed by push/pop operations triggered by fillWindow
            ctx.fillWindowTicks(ctx, posChunk.get(ii));
            localOutputValues.set(ii, ctx.currentVal);
        }
    }

    private void computeTime(@NotNull final Context ctx,
                             @NotNull final RowSequence inputKeys) {

        final WritableLongChunk<Values> localOutputValues = ctx.outputValues.get();
        // get the timestamp values for this chunk
        try (final ChunkSource.GetContext context = timestampColumnSource.makeGetContext(inputKeys.intSize())) {
            LongChunk timestampChunk = timestampColumnSource.getChunk(context, inputKeys).asLongChunk();

            for (int ii = 0; ii < inputKeys.intSize(); ii++) {
                // the output value is computed by push/pop operations triggered by fillWindow
                ctx.fillWindowTime(ctx, timestampChunk.get(ii));
                localOutputValues.set(ii, ctx.currentVal);
            }
        }
    }

    @NotNull
    @Override
    public Map<String, ColumnSource<?>> getOutputColumns() {
        return Collections.singletonMap(pair.leftColumn, outputSource);
    }

    @Override
    public void startTrackingPrev() {
        outputSource.startTrackingPrevValues();
        if(redirContext.isRedirected()) {
            maybeInnerSource.startTrackingPrevValues();
        }
    }

    @Override
    public void applyOutputShift(@NotNull final RowSet subIndexToShift, final long delta) {
        ((LongSparseArraySource)outputSource).shift(subIndexToShift, delta);
    }
}
