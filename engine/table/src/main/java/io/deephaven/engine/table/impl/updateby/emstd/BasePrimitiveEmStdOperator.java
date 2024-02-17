package io.deephaven.engine.table.impl.updateby.emstd;

import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.WritableSourceWithPrepareForParallelPopulation;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.sources.DoubleSparseArraySource;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public abstract class BasePrimitiveEmStdOperator extends BaseDoubleUpdateByOperator {
    protected final OperationControl control;
    /** For EM operators, we can allow floating-point tick/time units. */
    protected final double reverseWindowScaleUnits;
    protected final double opAlpha;
    protected double opOneMinusAlpha;

    protected final WritableColumnSource<Double> emaSource;
    protected final WritableColumnSource<Double> maybeEmaInnerSource;

    public abstract class Context extends BaseDoubleUpdateByOperator.Context {
        protected final ChunkSink.FillFromContext emaFillContext;
        protected final WritableDoubleChunk<Values> emaValues;

        protected double alpha;
        protected double oneMinusAlpha;
        protected long lastDt = NULL_LONG;
        protected long lastStamp = NULL_LONG;

        protected double curEma;
        protected double curVariance;

        Context(final int chunkSize) {
            super(chunkSize);
            if (emaSource != null) {
                this.emaFillContext = emaSource.makeFillFromContext(chunkSize);
                this.emaValues = WritableDoubleChunk.makeWritableChunk(chunkSize);
            } else {
                this.emaFillContext = null;
                this.emaValues = null;
            }
        }

        @Override
        public void push(int pos, int count) {
            throw new IllegalStateException("EmStdOperator#push() is not used");
        }

        @Override
        public void reset() {
            super.reset();
            curVariance = NULL_DOUBLE;
            curEma = NULL_DOUBLE;
            curVal = Double.NaN;
            lastStamp = NULL_LONG;
        }

        @Override
        public void close() {
            super.close();
            SafeCloseable.closeAll(emaValues, emaFillContext);
        }
    }

    /**
     * An operator that computes an EM Std output from an input column using an exponential decay function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param rowRedirection the row redirection to use for the EM operator output columns
     * @param control the control parameters for EM operator
     * @param timestampColumnName an optional timestamp column. If this is null, it will be assumed time is measured in
     *        integer ticks.
     * @param windowScaleUnits the smoothing window for the EM operator. If no {@code timestampColumnName} is provided,
     *        this is measured in ticks, otherwise it is measured in nanoseconds.
     */
    public BasePrimitiveEmStdOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final RowRedirection rowRedirection,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            final double windowScaleUnits,
            final boolean sourceRefreshing) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, 0, 0, false);
        this.control = control;
        this.reverseWindowScaleUnits = windowScaleUnits;

        if (sourceRefreshing) {
            if (rowRedirection != null) {
                // region create-dense
                this.maybeEmaInnerSource = new DoubleArraySource();
                // endregion create-dense
                this.emaSource = WritableRedirectedColumnSource.maybeRedirect(rowRedirection, maybeInnerSource, 0);
            } else {
                this.maybeEmaInnerSource = null;
                // region create-sparse
                this.emaSource = new DoubleSparseArraySource();
                // endregion create-sparse
            }
        } else {
            this.maybeEmaInnerSource = null;
            this.emaSource = null;
        }

        opAlpha = Math.exp(-1.0 / reverseWindowScaleUnits);
        opOneMinusAlpha = 1 - opAlpha;
    }

    @Override
    public void initializeCumulative(@NotNull final UpdateByOperator.Context updateContext,
            final long firstUnmodifiedKey,
            final long firstUnmodifiedTimestamp,
            @NotNull final RowSet bucketRowSet) {
        super.initializeCumulative(updateContext, firstUnmodifiedKey, firstUnmodifiedTimestamp, bucketRowSet);

        final Context ctx = (Context) updateContext;
        // rely on the caller to validate this is a valid timestamp (or NULL_LONG when appropriate)
        ctx.lastStamp = firstUnmodifiedTimestamp;
        if (firstUnmodifiedKey != NULL_ROW_KEY) {
            ctx.curVal = outputSource.getDouble(firstUnmodifiedKey);
            ctx.curEma = emaSource.getDouble(firstUnmodifiedKey);
            if (ctx.curEma != NULL_DOUBLE
                    && !Double.isNaN(ctx.curEma)
                    && Double.isNaN(ctx.curVal)) {
                // When we have a valid EMA, but the previous em_std value is NaN, we need to un-poison variance
                // (by setting to 0.0) to allow the new variance to be computed properly.
                // NOTE: this case can only exist between the first and second rows after initialization or a RESET
                // caused by the {@link OperationControl control}.
                ctx.curVariance = 0.0;
            } else if (ctx.curVal == NULL_DOUBLE) {
                ctx.curVariance = 0.0;
            } else {
                ctx.curVariance = Math.pow(ctx.curVal, 2.0);
            }
        }
    }

    @Override
    public void startTrackingPrev() {
        super.startTrackingPrev();
        if (emaSource != null) {
            emaSource.startTrackingPrevValues();
            if (rowRedirection != null) {
                assert maybeEmaInnerSource != null;
                maybeEmaInnerSource.startTrackingPrevValues();
            }
        }
    }

    // region Shifts
    @Override
    public void applyOutputShift(@NotNull final RowSet subRowSetToShift, final long delta) {
        super.applyOutputShift(subRowSetToShift, delta);
        if (emaSource != null) {
            ((DoubleSparseArraySource) emaSource).shift(subRowSetToShift, delta);
        }
    }
    // endregion Shifts

    @Override
    public void prepareForParallelPopulation(final RowSet changedRows) {
        super.prepareForParallelPopulation(changedRows);
        if (emaSource != null) {
            if (rowRedirection != null) {
                ((WritableSourceWithPrepareForParallelPopulation) maybeEmaInnerSource)
                        .prepareForParallelPopulation(changedRows);
            } else {
                ((WritableSourceWithPrepareForParallelPopulation) emaSource).prepareForParallelPopulation(changedRows);
            }
        }
    }

    protected void handleBadData(@NotNull final Context ctx,
            final boolean isNull,
            final boolean isNan) {
        boolean doReset = false;
        if (isNull) {
            if (control.onNullValueOrDefault() == BadDataBehavior.THROW) {
                throw new TableDataException("Encountered null value during Exponential Moving output processing");
            }
            doReset = control.onNullValueOrDefault() == BadDataBehavior.RESET;
        } else if (isNan) {
            if (control.onNanValueOrDefault() == BadDataBehavior.THROW) {
                throw new TableDataException("Encountered NaN value during Exponential Moving output processing");
            } else if (control.onNanValueOrDefault() == BadDataBehavior.POISON) {
                ctx.curVal = Double.NaN;
                ctx.curVariance = Double.NaN;
                ctx.curEma = Double.NaN;
            } else {
                doReset = control.onNanValueOrDefault() == BadDataBehavior.RESET;
            }
        }

        if (doReset) {
            ctx.reset();
        }
    }
}
