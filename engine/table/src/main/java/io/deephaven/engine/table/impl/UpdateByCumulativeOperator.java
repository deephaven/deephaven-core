package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.TableUpdate;
import org.jetbrains.annotations.NotNull;

public abstract class UpdateByCumulativeOperator implements UpdateByOperator {

    public abstract class UpdateCumulativeContext implements UpdateContext {
        // store the current subset of rows that need computation
        protected RowSet affectedRows = RowSetFactory.empty();

        public RowSet determineAffectedRows(@NotNull final TableUpdate upstream, @NotNull final RowSet source, final boolean upstreamAppendOnly) {
            if (upstreamAppendOnly) {
                // cumulative operators do not need to reprocess any rows on append-only updates
                try (final RowSet ignored = affectedRows) {
                    affectedRows = RowSetFactory.empty();
                }
                return affectedRows;
            }

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

        @Override
        public void close() {
            affectedRows.close();
        }
    }
}
