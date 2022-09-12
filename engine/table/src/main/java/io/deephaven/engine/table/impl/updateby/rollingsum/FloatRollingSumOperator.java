package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.base.LongRingBuffer;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedFloatChunk;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedFloatUpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedShortUpdateByOperator;
import io.deephaven.engine.table.impl.util.SizedSafeCloseable;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.mutable.MutableFloat;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public class FloatRollingSumOperator extends BaseWindowedFloatUpdateByOperator {

    // RollingSum will output Long values for integral types
    private final WritableColumnSource<Float> outputSource;
    private final WritableColumnSource<Float> maybeInnerSource;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseWindowedFloatUpdateByOperator.Context {
        public final SizedSafeCloseable<ChunkSink.FillFromContext> fillContext;
        public final SizedFloatChunk<Values> outputValues;

        // position data for the chunk being currently processed
        public SizedLongChunk<? extends RowKeys> valuePositionChunk;

        protected Context(final int chunkSize, final LongSegmentedSortedArray timestampSsa) {
            this.fillContext = new SizedSafeCloseable<>(outputSource::makeFillFromContext);
            this.fillContext.ensureCapacity(chunkSize);
            this.outputValues = new SizedFloatChunk<>(chunkSize);
            this.valuePositionChunk = new SizedLongChunk<>(chunkSize);
            this.timestampSsa = timestampSsa;
        }

        @Override
        public void close() {
            super.close();
            outputValues.close();
            fillContext.close();
            this.valuePositionChunk.close();
        }
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(final int chunkSize, final LongSegmentedSortedArray timestampSsa) {
        return new Context(chunkSize, timestampSsa);
    }

    @Override
    public void setChunkSize(@NotNull UpdateContext context, int chunkSize) {
        final Context ctx = (Context) context;
        ctx.outputValues.ensureCapacity(chunkSize);
        ctx.fillContext.ensureCapacity(chunkSize);
        ctx.valuePositionChunk.ensureCapacity(chunkSize);
    }

    public FloatRollingSumOperator(@NotNull final MatchPair pair,
                                   @NotNull final String[] affectingColumns,
                                   @NotNull final OperationControl control,
                                   @Nullable final String timestampColumnName,
                                   @Nullable final ColumnSource<?> timestampColumnSource,
                                   final long reverseTimeScaleUnits,
                                   final long forwardTimeScaleUnits,
                                   @NotNull final UpdateBy.UpdateByRedirectionContext redirContext,
                                   @NotNull final ColumnSource<Float> valueSource
                                   // region extra-constructor-args
                                   // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, control, timestampColumnName, timestampColumnSource, reverseTimeScaleUnits, forwardTimeScaleUnits, redirContext, valueSource);
        if(redirContext.isRedirected()) {
            // region create-dense
            this.maybeInnerSource = new FloatArraySource();
            // endregion create-dense
            this.outputSource = new WritableRedirectedColumnSource(redirContext.getRowRedirection(), maybeInnerSource, 0);
        } else {
            this.maybeInnerSource = null;
            // region create-sparse
            this.outputSource = new FloatSparseArraySource();
            // endregion create-sparse
        }

        // region constructor
        // endregion constructor
    }

    @Override
    public void push(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;
        float val = ctx.candidateValuesChunk.get(pos);
        if (val == NULL_FLOAT) {
            ctx.nullCount++;
        }
    }

    @Override
    public void pop(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;
        float val = ctx.candidateValuesChunk.get(pos);
        if (val == NULL_FLOAT) {
            ctx.nullCount--;
        }
    }

    @Override
    public void reset(UpdateContext context) {
        final Context ctx = (Context) context;
    }

    @Override
    public void doProcessChunk(@NotNull final BaseWindowedFloatUpdateByOperator.Context context,
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

        final WritableFloatChunk<Values> localOutputValues = ctx.outputValues.get();
        for (int ii = 0; ii < runLength; ii++) {
            ctx.fillWindowTicks(ctx, posChunk.get(ii));

            float sum = NULL_FLOAT;

            LongRingBuffer.Iterator it = ctx.windowIndices.iterator();
            while (it.hasNext()) {
                float v = ctx.candidateValuesChunk.get((int)it.next());
                if (v != QueryConstants.NULL_FLOAT) {
                    if (sum == NULL_FLOAT) {
                        sum = v;
                    } else {
                        sum += v;
                    }
                }
            }
            localOutputValues.set(ii, sum);
        }
    }

    private void computeTime(@NotNull final Context ctx,
                             @NotNull final RowSequence inputKeys) {

        final WritableFloatChunk<Values> localOutputValues = ctx.outputValues.get();
        // get the timestamp values for this chunk
        try (final ChunkSource.GetContext context = timestampColumnSource.makeGetContext(inputKeys.intSize())) {
            LongChunk timestampChunk = timestampColumnSource.getChunk(context, inputKeys).asLongChunk();

            for (int ii = 0; ii < inputKeys.intSize(); ii++) {
                long ts = timestampChunk.get(ii);

                // does this value have a valid timestamp
                if (ts == NULL_LONG) {
                    localOutputValues.set(ii, NULL_FLOAT);
                } else {
                    // the output value is computed by push/pop operations triggered by fillWindow
                    ctx.fillWindowTime(ctx, timestampChunk.get(ii));

                    float sum = NULL_FLOAT;

                    LongRingBuffer.Iterator it = ctx.windowIndices.iterator();
                    while (it.hasNext()) {
                        float v = ctx.candidateValuesChunk.get((int)it.next());
                        if (v != QueryConstants.NULL_FLOAT) {
                            if (sum == NULL_FLOAT) {
                                sum = v;
                            } else {
                                sum += v;
                            }
                        }
                    }
                    localOutputValues.set(ii, sum);
                }
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
    public void applyOutputShift(@NotNull final UpdateContext context,
                                 @NotNull final RowSet subIndexToShift,
                                 final long delta) {
        ((FloatSparseArraySource)outputSource).shift(subIndexToShift, delta);
    }
}
