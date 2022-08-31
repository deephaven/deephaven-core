package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.chunk.util.pools.ChunkPoolConstants;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.updateby.internal.*;
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

        // position data for the chunk being currently processed
        public SizedLongChunk<? extends RowKeys> valuePositionChunk;

        protected Context(final int chunkSize) {
            this.fillContext = new SizedSafeCloseable<>(outputSource::makeFillFromContext);
            this.fillContext.ensureCapacity(chunkSize);
            this.outputValues = new SizedLongChunk<>(chunkSize);
            this.valuePositionChunk = new SizedLongChunk<>(chunkSize);
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
    public UpdateByOperator.UpdateContext makeUpdateContext(final int chunkSize) {
        return new Context(chunkSize);
    }

    @Override
    public void setChunkSize(@NotNull UpdateContext context, int chunkSize) {
        final Context ctx = (Context) context;
        ctx.outputValues.ensureCapacity(chunkSize);
        ctx.fillContext.ensureCapacity(chunkSize);
        ctx.valuePositionChunk.ensureCapacity(chunkSize);
    }

    public ShortRollingSumOperator(@NotNull final MatchPair pair,
                                   @NotNull final String[] affectingColumns,
                                   @NotNull final OperationControl control,
                                   @Nullable final LongRecordingUpdateByOperator recorder,
                                   @Nullable final String timestampColumnName,
                                   final long reverseTimeScaleUnits,
                                   final long forwardTimeScaleUnits,
                                   @NotNull final ColumnSource<Short> valueSource,
                                   @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                   // region extra-constructor-args
                                   // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, control, recorder, timestampColumnName, reverseTimeScaleUnits, forwardTimeScaleUnits, redirContext, valueSource);
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
        Short val = ctx.candidateValuesChunk.get(pos);

        // increase the running sum
        if (val != NULL_SHORT) {
            if (ctx.currentVal == NULL_LONG) {
                ctx.currentVal = val;
            } else {
                ctx.currentVal += val;
            }
        }
    }

    @Override
    public void pop(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;
        Short val = ctx.candidateValuesChunk.get(pos);

        // reduce the running sum
        if (val != NULL_SHORT) {
            ctx.currentVal -= val;
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
                              @NotNull final Chunk<Values> workingChunk) {
        final Context ctx = (Context) context;

        if (timestampColumnName == null) {
            // produce position data for the window (will be timestamps for time-based)
            // TODO: gotta be a better way than creating two rowsets
            try (final RowSet rs = inputKeys.asRowSet();
                 final RowSet positions = ctx.sourceRowSet.invert(rs)) {
                positions.fillRowKeyChunk(ctx.valuePositionChunk.get());
            }
        }

        computeTicks(ctx, 0, inputKeys.intSize());

        //noinspection unchecked
        outputSource.fillFromChunk(ctx.fillContext.get(), ctx.outputValues.get(), inputKeys);
    }

    private void computeTicks(@NotNull final Context ctx,
                              final int runStart,
                              final int runLength) {

        final WritableLongChunk<Values> localOutputValues = ctx.outputValues.get();
        for (int ii = runStart; ii < runStart + runLength; ii++) {
            if (recorder == null) {
                ctx.fillWindowTicks(ctx, ctx.valuePositionChunk.get().get(ii));
            }
            // the sum was computed by push/pop operations
            localOutputValues.set(ii, ctx.currentVal);
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
        ((LongSparseArraySource)outputSource).shift(subIndexToShift, delta);
    }
}
