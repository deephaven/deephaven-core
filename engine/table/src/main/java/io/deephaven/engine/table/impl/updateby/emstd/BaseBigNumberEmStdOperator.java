//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.emstd;

import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.ObjectSparseArraySource;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.MathContext;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public abstract class BaseBigNumberEmStdOperator<T> extends BaseObjectUpdateByOperator<BigDecimal> {
    protected final OperationControl control;
    /** For EM operators, we can allow floating-point tick/time units. */
    protected final double reverseWindowScaleUnits;

    @NotNull
    protected final MathContext mathContext;

    protected final BigDecimal opAlpha;
    protected final BigDecimal opOneMinusAlpha;

    protected ColumnSource<?> valueSource;
    protected WritableColumnSource<BigDecimal> emaSource;
    protected WritableColumnSource<BigDecimal> maybeEmaInnerSource;

    public interface EmFunction {
        BigDecimal apply(BigDecimal prevVal, BigDecimal curVal, BigDecimal alpha, BigDecimal oneMinusAlpha);
    }

    public abstract class Context extends BaseObjectUpdateByOperator<BigDecimal>.Context {
        protected ObjectChunk<T, ? extends Values> objectValueChunk;

        protected final ChunkSink.FillFromContext emaFillContext;
        protected final WritableObjectChunk<BigDecimal, ? extends Values> emaValues;

        protected BigDecimal alpha;
        protected BigDecimal oneMinusAlpha;
        protected long lastDt = NULL_LONG;
        protected long lastStamp = NULL_LONG;

        protected BigDecimal curEma;
        protected BigDecimal curVariance;

        protected Context(final int chunkSize) {
            super(chunkSize);
            if (emaSource != null) {
                this.emaFillContext = emaSource.makeFillFromContext(chunkSize);
                this.emaValues = WritableObjectChunk.makeWritableChunk(chunkSize);
            } else {
                this.emaFillContext = null;
                this.emaValues = null;
            }
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            objectValueChunk = valueChunks[0].asObjectChunk();
        }

        @Override
        public void writeToOutputColumn(@NotNull final RowSequence inputKeys) {
            outputSource.fillFromChunk(outputFillContext, outputValues, inputKeys);
            if (emaValues != null) {
                emaSource.fillFromChunk(emaFillContext, emaValues, inputKeys);
            }
        }

        @Override
        public boolean isValueValid(long atKey) {
            return valueSource.get(atKey) != null;
        }

        @Override
        public void push(int pos, int count) {
            throw new IllegalStateException("EmStdOperator#push() is not used");
        }

        @Override
        public void reset() {
            lastStamp = NULL_LONG;
            lastDt = NULL_LONG;
            alpha = null;
            oneMinusAlpha = null;
            curVariance = BigDecimal.ZERO;
            curEma = null;
            curVal = null;
        }

        @Override
        public void close() {
            super.close();
            SafeCloseable.closeAll(emaValues, emaFillContext);
        }
    }

    /**
     * An operator that computes an EM Std output from a big number column using an exponential decay function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param control defines how to handle {@code null} input values.
     * @param timestampColumnName the name of the column containing timestamps for time-based calcuations
     * @param windowScaleUnits the smoothing window for the EMA. If no {@code timestampColumnName} is provided, this is
     *        measured in ticks, otherwise it is measured in nanoseconds
     */
    public BaseBigNumberEmStdOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            final double windowScaleUnits,
            @NotNull final MathContext mathContext) {
        super(pair, affectingColumns, timestampColumnName, 0, 0, false, BigDecimal.class);

        this.control = control;
        this.mathContext = mathContext;
        this.reverseWindowScaleUnits = windowScaleUnits;

        if (timestampColumnName == null) {
            // tick-based, pre-compute alpha and oneMinusAlpha
            opAlpha = computeAlpha(-1, reverseWindowScaleUnits);
            opOneMinusAlpha = computeOneMinusAlpha(opAlpha);
        } else {
            // time-based, must compute alpha and oneMinusAlpha for each time delta
            opAlpha = null;
            opOneMinusAlpha = null;
        }
    }

    @Override
    public void initializeSources(@NotNull final Table source, @Nullable final RowRedirection rowRedirection) {
        super.initializeSources(source, rowRedirection);

        valueSource = source.getColumnSource(pair.rightColumn);

        if (source.isRefreshing()) {
            if (rowRedirection != null) {
                // region create-dense
                maybeEmaInnerSource = new ObjectArraySource<>(BigDecimal.class);
                // endregion create-dense
                emaSource = WritableRedirectedColumnSource.maybeRedirect(rowRedirection, maybeInnerSource, 0);
            } else {
                maybeEmaInnerSource = null;
                // region create-sparse
                emaSource = new ObjectSparseArraySource<>(BigDecimal.class);
                // endregion create-sparse
            }
        } else {
            maybeEmaInnerSource = null;
            emaSource = null;
        }
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
            ctx.curVal = outputSource.get(firstUnmodifiedKey);
            ctx.curEma = emaSource.get(firstUnmodifiedKey);
            if (ctx.curVal == null) {
                ctx.curVariance = BigDecimal.ZERO;
            } else {
                ctx.curVariance = ctx.curVal.pow(2);
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
            ((ObjectSparseArraySource) emaSource).shift(subRowSetToShift, delta);
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

    protected void handleBadData(@NotNull final Context ctx, final boolean isNull) {
        boolean doReset = false;
        if (isNull) {
            if (control.onNullValueOrDefault() == BadDataBehavior.THROW) {
                throw new TableDataException("Encountered invalid data during EM processing");
            }
            doReset = control.onNullValueOrDefault() == BadDataBehavior.RESET;
        }

        if (doReset) {
            ctx.reset();
        }
    }

    protected BigDecimal computeAlpha(final long dt, final double timeScaleUnits) {
        return BigDecimal.valueOf(Math.exp(dt / timeScaleUnits));
    }

    protected BigDecimal computeOneMinusAlpha(final BigDecimal alpha) {
        return BigDecimal.ONE.subtract(alpha, control.bigValueContextOrDefault());
    }
}
