package io.deephaven.engine.table.impl.updateby.emsum;

import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public abstract class BaseBigNumberEMSOperator<T> extends BaseObjectUpdateByOperator<BigDecimal> {
    protected final ColumnSource<?> valueSource;
    protected final OperationControl control;

    protected final BigDecimal opAlpha;

    public abstract class Context extends BaseObjectUpdateByOperator<BigDecimal>.Context {
        public ObjectChunk<T, ? extends Values> objectValueChunk;

        protected BigDecimal alpha;
        protected long lastDt = NULL_LONG;
        protected long lastStamp = NULL_LONG;

        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            objectValueChunk = valuesChunk.asObjectChunk();
        }

        @Override
        public boolean isValueValid(long atKey) {
            return valueSource.get(atKey) != null;
        }

        @Override
        public void push(int pos, int count) {
            throw Assert.statementNeverExecuted("EMSOperator#push() is not used");
        }

        @Override
        public void reset() {
            curVal = null;
            lastStamp = NULL_LONG;
            lastDt = NULL_LONG;
            alpha = null;
        }
    }

    /**
     * An operator that computes an EM Sum from a big number column using an exponential decay function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param rowRedirection the {@link RowRedirection} to use for dense output sources
     * @param control defines how to handle {@code null} input values.
     * @param timestampColumnName the name of the column containing timestamps for time-based calcuations
     * @param windowScaleUnits the smoothing window for the EMS. If no {@code timestampColumnName} is provided, this is
     *        measured in ticks, otherwise it is measured in nanoseconds
     * @param valueSource a reference to the input column source for this operation
     */
    public BaseBigNumberEMSOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final RowRedirection rowRedirection,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            final long windowScaleUnits,
            @NotNull final ColumnSource<?> valueSource) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, windowScaleUnits, 0, false,
                BigDecimal.class);

        this.control = control;
        this.valueSource = valueSource;

        if (timestampColumnName == null) {
            // tick-based, pre-compute alpha and oneMinusAlpha
            opAlpha = computeAlpha(-1, windowScaleUnits);
        } else {
            // time-based, must compute alpha and oneMinusAlpha for each time delta
            opAlpha = null;
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
    }

    void handleBadData(@NotNull final Context ctx, final boolean isNull) {
        boolean doReset = false;
        if (isNull) {
            if (control.onNullValueOrDefault() == BadDataBehavior.THROW) {
                throw new TableDataException("Encountered invalid data during EMS processing");
            }
            doReset = control.onNullValueOrDefault() == BadDataBehavior.RESET;
        }

        if (doReset) {
            ctx.reset();
        }
    }

    BigDecimal computeAlpha(final long dt, final long timeScaleUnits) {
        return BigDecimal.valueOf(Math.exp(dt / (double) timeScaleUnits));
    }
}
