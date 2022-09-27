package io.deephaven.engine.table.impl.updateby.ema;

import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.*;

public abstract class BigNumberEMAOperator<T> extends BaseObjectUpdateByOperator<BigDecimal> {
    protected final OperationControl control;
    protected final double timeScaleUnits;
    protected final BigDecimal alpha;
    protected final BigDecimal oneMinusAlpha;


    class Context extends BaseObjectUpdateByOperator<BigDecimal>.Context {
        protected final ColumnSource<?> valueSource;
        public LongChunk<? extends Values> timestampValueChunk;
        public ObjectChunk<T, Values> objectValueChunk;

        long lastStamp = NULL_LONG;

        protected Context(int chunkSize, ColumnSource<?> inputSource) {
            super(chunkSize);
            this.valueSource = inputSource;
        }

        @Override
        public void storeValuesChunk(@NotNull final Chunk<Values> valuesChunk) {
            objectValueChunk = valuesChunk.asObjectChunk();
        }

        @Override
        public boolean isValueValid(long atKey) {
            return valueSource.get(atKey) != null;
        }
    }

    /**
     * An operator that computes an EMA from a int column using an exponential decay function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param control defines how to handle {@code null} input values.
     * @param timeScaleUnits the smoothing window for the EMA. If no {@code timeRecorder} is provided, this is measured
     *        in ticks, otherwise it is measured in nanoseconds
     */
    public BigNumberEMAOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            final long timeScaleUnits,
            @NotNull final UpdateBy.UpdateByRedirectionContext redirContext) {
        super(pair, affectingColumns, redirContext, BigDecimal.class);

        this.control = control;
        this.timestampColumnName = timestampColumnName;
        this.timeScaleUnits = (double) timeScaleUnits;

        alpha = BigDecimal.valueOf(Math.exp(-1.0 / (double) timeScaleUnits));
        oneMinusAlpha =
                timestampColumnName == null ? BigDecimal.ONE.subtract(alpha, control.bigValueContextOrDefault()) : null;
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(final int chunkSize, ColumnSource<?> inputSource) {
        return new Context(chunkSize, inputSource);
    }

    @Override
    public void initializeUpdate(@NotNull final UpdateContext updateContext,
            @NotNull final long firstUnmodifiedKey, long firstUnmodifiedTimestamp) {
        super.initializeUpdate(updateContext, firstUnmodifiedKey, firstUnmodifiedTimestamp);

        final Context ctx = (Context) updateContext;
        // If we set the last state to null, then we know it was a reset state and the timestamp must also
        // have been reset.
        if (ctx.curVal == null || (firstUnmodifiedKey == NULL_ROW_KEY)) {
            ctx.lastStamp = NULL_LONG;
        } else {
            // rely on the caller to validate this is a valid timestamp (or NULL_LONG when appropriate)
            ctx.lastStamp = firstUnmodifiedTimestamp;
        }
    }

    @Override
    public void processChunk(@NotNull final UpdateContext updateContext,
            @NotNull final RowSequence inputKeys,
            @Nullable final LongChunk<OrderedRowKeys> keyChunk,
            @Nullable final LongChunk<OrderedRowKeys> posChunk,
            @Nullable final Chunk<Values> valuesChunk,
            @Nullable final LongChunk<? extends Values> timestampValuesChunk) {
        Assert.neqNull(valuesChunk, "valuesChunk must not be null for a cumulative operator");
        final Context ctx = (Context) updateContext;
        ctx.storeValuesChunk(valuesChunk);
        ctx.timestampValueChunk = timestampValuesChunk;
        for (int ii = 0; ii < valuesChunk.size(); ii++) {
            push(ctx, keyChunk == null ? NULL_ROW_KEY : keyChunk.get(ii), ii);
            ctx.outputValues.set(ii, ctx.curVal);
        }
        outputSource.fillFromChunk(ctx.fillContext, ctx.outputValues, inputKeys);
    }

    void handleBadData(@NotNull final Context ctx,
            final boolean isNull,
            final boolean isNullTime) {
        boolean doReset = false;
        if (isNull) {
            if (control.onNullValueOrDefault() == BadDataBehavior.THROW) {
                throw new TableDataException("Encountered invalid data during EMA processing");
            }
            doReset = control.onNullValueOrDefault() == BadDataBehavior.RESET;
        }

        if (isNullTime) {
            if (control.onNullTimeOrDefault() == BadDataBehavior.THROW) {
                throw new TableDataException("Encountered null timestamp during EMA processing");
            }
            doReset = control.onNullTimeOrDefault() == BadDataBehavior.RESET;
        }

        if (doReset) {
            ctx.curVal = null;
            ctx.lastStamp = NULL_LONG;
        }
    }

    void handleBadTime(@NotNull final Context ctx, final long dt) {
        boolean doReset = false;
        if (dt == 0) {
            if (control.onZeroDeltaTimeOrDefault() == BadDataBehavior.THROW) {
                throw new TableDataException("Encountered zero delta time during EMA processing");
            }
            doReset = control.onZeroDeltaTimeOrDefault() == BadDataBehavior.RESET;
        } else if (dt < 0) {
            if (control.onNegativeDeltaTimeOrDefault() == BadDataBehavior.THROW) {
                throw new TableDataException("Encountered negative delta time during EMA processing");
            }
            doReset = control.onNegativeDeltaTimeOrDefault() == BadDataBehavior.RESET;
        }

        if (doReset) {
            ctx.curVal = null;
            ctx.lastStamp = NULL_LONG;
        }
    }
}
