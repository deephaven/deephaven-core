package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.engine.table.impl.updateby.internal.BaseCharUpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedByteUpdateByOperator;
import io.deephaven.tablelogger.Row;
import org.jetbrains.annotations.NotNull;

public abstract class UpdateByCumulativeOperator implements UpdateByOperator {

    public abstract class UpdateCumulativeContext implements UpdateContext {
        protected LongSegmentedSortedArray timestampSsa;

        protected RowSetBuilderSequential modifiedBuilder;
        protected RowSet newModified;

        public LongSegmentedSortedArray getTimestampSsa() {
            return timestampSsa;
        }

        public RowSetBuilderSequential getModifiedBuilder() {
            if (modifiedBuilder == null) {
                modifiedBuilder = RowSetFactory.builderSequential();
            }
            return modifiedBuilder;
        }

        @Override
        public void close() {
            try (final RowSet ignored = affectedRows;
                    final RowSet ignored2 = newModified) {
            }
        }
    }

    @Override
    public void initializeFor(@NotNull final UpdateContext context,
            @NotNull final RowSet updateRowSet) {}

    @Override
    public void finishFor(@NotNull final UpdateContext context) {
        UpdateCumulativeContext ctx = (UpdateCumulativeContext) context;
        ctx.newModified = ctx.getModifiedBuilder().build();
    }

    @NotNull
    final public RowSet getAdditionalModifications(@NotNull final UpdateContext context) {
        UpdateCumulativeContext ctx = (UpdateCumulativeContext) context;
        return ctx.newModified;
    }

    @Override
    final public boolean anyModified(@NotNull final UpdateContext context) {
        UpdateCumulativeContext ctx = (UpdateCumulativeContext) context;
        return ctx.newModified != null && ctx.newModified.isNonempty();
    }

    /*** nearly all cumulative operators will not reference a timestamp column, exceptions are Ema */
    @Override
    public String getTimestampColumnName() {
        return null;
    }

    /**
     * Get the value of the backward-looking window (might be nanos or ticks).
     *
     * @return the name of the input column
     */
    @Override
    public long getPrevWindowUnits() {
        return 0L;
    }

    /**
     * Get the value of the forward-looking window (might be nanos or ticks).
     *
     * @return the name of the input column
     */
    @Override
    public long getFwdWindowUnits() {
        return 0L;
    }

    /*** cumulative operators do not need keys */
    @Override
    public boolean requiresKeys() {
        return false;
    }

    /*** cumulative operators do not need position data */
    @Override
    public boolean requiresPositions() {
        return false;
    }

    /*** cumulative operators always need values */
    @Override
    public boolean requiresValues(@NotNull final UpdateContext context) {
        return true;
    }
}
