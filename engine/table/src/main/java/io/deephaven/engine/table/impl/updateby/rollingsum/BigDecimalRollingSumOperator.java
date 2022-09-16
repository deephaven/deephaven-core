package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedObjectChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedObjectUpdateByOperator;
import io.deephaven.engine.table.impl.util.SizedSafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Collections;
import java.util.Map;

public final class BigDecimalRollingSumOperator extends BaseWindowedObjectUpdateByOperator<BigDecimal> {
    @NotNull
    private final MathContext mathContext;

    private final WritableColumnSource<BigDecimal> outputSource;
    private final WritableColumnSource<BigDecimal> maybeInnerSource;

    protected class Context extends BaseWindowedObjectUpdateByOperator<BigDecimal>.Context {
        public final SizedSafeCloseable<ChunkSink.FillFromContext> fillContext;
        public final SizedObjectChunk<BigDecimal, Values> outputValues;

        public BigDecimal currentVal = null;

        protected Context(final int chunkSize, final LongSegmentedSortedArray timestampSsa) {
            this.fillContext = new SizedSafeCloseable<>(outputSource::makeFillFromContext);
            this.fillContext.ensureCapacity(chunkSize);
            this.outputValues = new SizedObjectChunk<>(chunkSize);
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
    public UpdateByOperator.UpdateContext makeUpdateContext(final int chunkSize,
            final LongSegmentedSortedArray timestampSsa) {
        return new Context(chunkSize, timestampSsa);
    }

    @Override
    public void setChunkSize(@NotNull UpdateContext context, int chunkSize) {
        final Context ctx = (Context) context;
        ctx.outputValues.ensureCapacity(chunkSize);
        ctx.fillContext.ensureCapacity(chunkSize);
    }


    public BigDecimalRollingSumOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            @Nullable final ColumnSource<?> timestampColumnSource,
            final long reverseTimeScaleUnits,
            final long forwardTimeScaleUnits,
            @NotNull final UpdateBy.UpdateByRedirectionContext redirContext,
            @NotNull final ColumnSource<Object> valueSource
            // region extra-constructor-args
            // endregion extra-constructor-args
            , @NotNull final MathContext mathContext) {
        super(pair, affectingColumns, control, timestampColumnName, timestampColumnSource, reverseTimeScaleUnits,
                forwardTimeScaleUnits, redirContext, valueSource, BigDecimal.class);
        this.mathContext = mathContext;
        if (redirContext.isRedirected()) {
            // region create-dense
            this.maybeInnerSource = new ObjectArraySource<BigDecimal>(BigDecimal.class);
            // endregion create-dense
            this.outputSource =
                    new WritableRedirectedColumnSource(redirContext.getRowRedirection(), maybeInnerSource, 0);
        } else {
            this.maybeInnerSource = null;
            // region create-sparse
            this.outputSource = new ObjectSparseArraySource<BigDecimal>(BigDecimal.class);
            // endregion create-sparse
        }

        // region constructor
        // endregion constructor
    }

    @Override
    public void push(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;
        BigDecimal val = (BigDecimal) ctx.candidateValuesChunk.get(pos);

        // increase the running sum
        if (val != null) {
            if (ctx.currentVal == null) {
                ctx.currentVal = val;
            } else {
                ctx.currentVal = ctx.currentVal.add(val, mathContext);
            }
        } else {
            ctx.nullCount++;
        }
    }

    @Override
    public void pop(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;
        BigDecimal val = (BigDecimal) ctx.candidateValuesChunk.get(pos);

        // reduce the running sum
        if (val != null) {
            ctx.currentVal = ctx.currentVal.subtract(val, mathContext);
        } else {
            ctx.nullCount--;
        }
    }

    @Override
    public void reset(UpdateContext context) {
        final Context ctx = (Context) context;
        ctx.currentVal = null;
    }

    @Override
    public void doProcessChunk(@NotNull final BaseWindowedObjectUpdateByOperator<BigDecimal>.Context context,
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

        // noinspection unchecked
        outputSource.fillFromChunk(ctx.fillContext.get(), ctx.outputValues.get(), inputKeys);
    }

    private void computeTicks(@NotNull final Context ctx,
            @Nullable final LongChunk<OrderedRowKeys> posChunk,
            final int runLength) {

        final WritableObjectChunk<BigDecimal, Values> localOutputValues = ctx.outputValues.get();
        for (int ii = 0; ii < runLength; ii++) {
            // the output value is computed by push/pop operations triggered by fillWindow
            ctx.fillWindowTicks(ctx, posChunk.get(ii));
            localOutputValues.set(ii, ctx.currentVal);
        }
    }

    private void computeTime(@NotNull final Context ctx,
            @NotNull final RowSequence inputKeys) {

        final WritableObjectChunk<BigDecimal, Values> localOutputValues = ctx.outputValues.get();
        // get the timestamp values for this chunk
        try (final ChunkSource.GetContext context = timestampColumnSource.makeGetContext(inputKeys.intSize())) {
            LongChunk<? extends Values> timestampChunk =
                    timestampColumnSource.getChunk(context, inputKeys).asLongChunk();

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
        if (redirContext.isRedirected()) {
            maybeInnerSource.startTrackingPrevValues();
        }
    }

    @Override
    public void applyOutputShift(@NotNull final RowSet subIndexToShift, final long delta) {
        ((ObjectSparseArraySource<BigDecimal>) outputSource).shift(subIndexToShift, delta);
    }
}
