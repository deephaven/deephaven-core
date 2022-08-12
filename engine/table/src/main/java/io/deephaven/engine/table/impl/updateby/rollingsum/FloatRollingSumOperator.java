package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.api.updateby.OperationControl;
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
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedFloatUpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.LongRecordingUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.impl.util.SizedSafeCloseable;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;

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
        public UpdateBy.UpdateType currentUpdateType;

        public LinkedList<Float> windowValues = new LinkedList<>();

        protected Context(final int chunkSize) {
            this.fillContext = new SizedSafeCloseable<>(outputSource::makeFillFromContext);
            this.fillContext.ensureCapacity(chunkSize);
            this.outputValues = new SizedFloatChunk<>(chunkSize);
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
    public UpdateContext makeUpdateContext(final int chunkSize) {
        return new Context(chunkSize);
    }

    @Override
    public void setChunkSize(@NotNull UpdateContext context, int chunkSize) {
        ((Context)context).outputValues.ensureCapacity(chunkSize);
        ((Context)context).fillContext.ensureCapacity(chunkSize);
    }

    public FloatRollingSumOperator(@NotNull final MatchPair pair,
                                   @NotNull final String[] affectingColumns,
                                   @NotNull final OperationControl control,
                                   @Nullable final LongRecordingUpdateByOperator recorder,
                                   final long reverseTimeScaleUnits,
                                   final long forwardTimeScaleUnits,
                                   @NotNull final ColumnSource<Float> valueSource,
                                   @Nullable final RowRedirection rowRedirection
                                   // region extra-constructor-args
                                   // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, control, recorder, reverseTimeScaleUnits, forwardTimeScaleUnits, rowRedirection, valueSource);
        if(rowRedirection != null) {
            // region create-dense
            this.maybeInnerSource = new FloatArraySource();
            // endregion create-dense
            this.outputSource = new WritableRedirectedColumnSource(rowRedirection, maybeInnerSource, 0);
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
    public void push(UpdateContext context, long key, float val) {
        final Context ctx = (Context) context;
        ctx.windowValues.addLast(val);
    }

    @Override
    public void pop(UpdateContext context, long key) {
        final Context ctx = (Context) context;
        ctx.windowValues.pop();
    }

    @Override
    public void reset(UpdateContext context) {
        final Context ctx = (Context) context;
    }

    @Override
    public void doAddChunk(@NotNull final BaseWindowedFloatUpdateByOperator.Context context,
                              @NotNull final RowSequence inputKeys,
                              @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                              @NotNull final Chunk<Values> workingChunk) {
        final Context ctx = (Context) context;

        computeTicks(ctx, 0, inputKeys.intSize());
        //noinspection unchecked
        outputSource.fillFromChunk(ctx.fillContext.get(), ctx.outputValues.get(), inputKeys);
    }

    private void computeTicks(@NotNull final Context ctx,
                              final int runStart,
                              final int runLength) {

        final WritableFloatChunk<Values> localOutputValues = ctx.outputValues.get();
        for (int ii = runStart; ii < runStart + runLength; ii++) {
            if (recorder == null) {
                ctx.fillWindowTicks(ctx, ctx.valuePositionChunk.get(ii));
            }

            MutableLong sum = new MutableLong(NULL_LONG);
            ctx.windowValues.forEach(v-> {
                if (v != null && v != QueryConstants.NULL_SHORT) {
                    if (sum.longValue() == NULL_LONG) {
                        sum.setValue(v);
                    } else {
                        sum.add(v);
                    }
                }
            });

            // this call generates the push/pop calls to satisfy the window
//            ctx.fillWindow(key, postUpdateSourceIndex);

            localOutputValues.set(ii, sum.getValue());
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
        if(isRedirected) {
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
