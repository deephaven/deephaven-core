/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit FloatRollingSumOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedDoubleChunk;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.util.SizedSafeCloseable;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DoubleRollingSumOperator extends BaseWindowedDoubleUpdateByOperator {

    // RollingSum will output Long values for integral types
    private final WritableColumnSource<Double> outputSource;
    private final WritableColumnSource<Double> maybeInnerSource;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseWindowedDoubleUpdateByOperator.Context {
        public final SizedSafeCloseable<ChunkSink.FillFromContext> fillContext;
        public final SizedDoubleChunk<Values> outputValues;

        public LinkedList<Double> windowValues = new LinkedList<>();

        // position data for the chunk being currently processed
        public SizedLongChunk<? extends RowKeys> valuePositionChunk;

        protected Context(final int chunkSize, final LongSegmentedSortedArray timestampSsa) {
            this.fillContext = new SizedSafeCloseable<>(outputSource::makeFillFromContext);
            this.fillContext.ensureCapacity(chunkSize);
            this.outputValues = new SizedDoubleChunk<>(chunkSize);
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

    public DoubleRollingSumOperator(@NotNull final MatchPair pair,
                                   @NotNull final String[] affectingColumns,
                                   @NotNull final OperationControl control,
                                   @Nullable final String timestampColumnName,
                                   @Nullable final ColumnSource<?> timestampColumnSource,
                                   final long reverseTimeScaleUnits,
                                   final long forwardTimeScaleUnits,
                                   @NotNull final ColumnSource<Double> valueSource,
                                   @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                   // region extra-constructor-args
                                   // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, control, timestampColumnName, timestampColumnSource, reverseTimeScaleUnits, forwardTimeScaleUnits, redirContext, valueSource);
        if(redirContext.isRedirected()) {
            // region create-dense
            this.maybeInnerSource = new DoubleArraySource();
            // endregion create-dense
            this.outputSource = new WritableRedirectedColumnSource(redirContext.getRowRedirection(), maybeInnerSource, 0);
        } else {
            this.maybeInnerSource = null;
            // region create-sparse
            this.outputSource = new DoubleSparseArraySource();
            // endregion create-sparse
        }

        // region constructor
        // endregion constructor
    }

    @Override
    public void push(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;
        double val = ctx.candidateValuesChunk.get(pos);
        ctx.windowValues.addLast(val);
    }

    @Override
    public void pop(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;
        double val = ctx.candidateValuesChunk.get(pos);
        ctx.windowValues.pop();
    }

    @Override
    public void reset(UpdateContext context) {
        final Context ctx = (Context) context;
    }

    @Override
    public void doProcessChunk(@NotNull final BaseWindowedDoubleUpdateByOperator.Context context,
                               @NotNull final RowSequence inputKeys,
                               @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                               @Nullable final LongChunk<OrderedRowKeys> posChunk,
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

        final WritableDoubleChunk<Values> localOutputValues = ctx.outputValues.get();
        for (int ii = runStart; ii < runStart + runLength; ii++) {
            if (timestampColumnName == null) {
                ctx.fillWindowTicks(ctx, ctx.valuePositionChunk.get().get(ii));
            }

            MutableDouble sum = new MutableDouble(NULL_DOUBLE);
            ctx.windowValues.forEach(v-> {
                if (v != QueryConstants.NULL_DOUBLE) {
                    if (sum.getValue() == NULL_DOUBLE) {
                        sum.setValue(v);
                    } else {
                        sum.add(v);
                    }
                }
            });

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
        if(redirContext.isRedirected()) {
            maybeInnerSource.startTrackingPrevValues();
        }
    }

    @Override
    public void applyOutputShift(@NotNull final UpdateContext context,
                                 @NotNull final RowSet subIndexToShift,
                                 final long delta) {
        ((DoubleSparseArraySource)outputSource).shift(subIndexToShift, delta);
    }
}
