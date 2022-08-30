package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.updateby.internal.BaseCharUpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedByteUpdateByOperator;
import io.deephaven.tablelogger.Row;
import org.jetbrains.annotations.NotNull;

public abstract class UpdateByCumulativeOperator implements UpdateByOperator {

    public abstract class UpdateCumulativeContext implements UpdateContext {
        protected RowSetBuilderSequential modifiedBuilder;
        protected RowSet newModified;

        // store the current subset of rows that need computation
        protected RowSet affectedRows;

        public RowSet determineAffectedRows(@NotNull final TableUpdate upstream, @NotNull final TrackingRowSet source, final boolean upstreamAppendOnly) {

            long smallestModifiedKey = UpdateByOperator.smallestAffectedKey(upstream.added(), upstream.modified(), upstream.removed(), upstream.shifted(), source);

            try (final RowSet ignored = affectedRows) {
                affectedRows = smallestModifiedKey == Long.MAX_VALUE
                        ? RowSetFactory.empty()
                        : source.subSetByKeyRange(smallestModifiedKey, source.lastRowKey());
            }
            return affectedRows;
        }

        public RowSet getAffectedRows() {
            return affectedRows;
        }

        public RowSetBuilderSequential getModifiedBuilder() {
            if(modifiedBuilder == null) {
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
                              @NotNull final RowSet updateRowSet) {
    }

    @Override
    public void finishFor(@NotNull final UpdateContext context) {
        UpdateCumulativeContext ctx = (UpdateCumulativeContext)context;
        ctx.newModified = ctx.getModifiedBuilder().build();
    }

    @NotNull
    final public RowSet getAdditionalModifications(@NotNull final UpdateContext context) {
        UpdateCumulativeContext ctx = (UpdateCumulativeContext)context;
        return ctx.newModified;
    }

    @Override
    final public boolean anyModified(@NotNull final UpdateContext context) {
        UpdateCumulativeContext ctx = (UpdateCumulativeContext)context;
        return ctx.newModified != null && ctx.newModified.isNonempty();
    }

    /*** nearly all cumulative operators will not reference a timestamp column, exceptions are Ema */
    @Override
    public String getTimestampColumnName() {
        return null;
    }

    /*** cumulative operators do not need keys */
    @Override
    public boolean requiresKeys() {
        return false;
    }

    /*** cumulative operators always need values */
    @Override
    public boolean requiresValues(@NotNull final UpdateContext context) {
        return true;
    }
}
