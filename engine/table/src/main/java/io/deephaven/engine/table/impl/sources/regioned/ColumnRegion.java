//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Releasable;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.page.Page;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownFilterMatcher;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

public interface ColumnRegion<ATTR extends Any> extends Page<ATTR>, Releasable, PushdownFilterMatcher {

    @Override
    @FinalDefault
    default long firstRowOffset() {
        return 0;
    }

    /**
     * Invalidate the region -- any further reads that cannot be completed consistently and correctly will fail.
     */
    void invalidate();

    abstract class Null<ATTR extends Any>
            extends GenericColumnRegionBase<ATTR>
            implements ColumnRegion<ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        Null(final long pageMask) {
            super(pageMask);
        }

        @Override
        public void fillChunkAppend(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super ATTR> destination, final int length) {
            final int offset = destination.size();

            destination.fillWithNullValue(offset, length);
            destination.setSize(offset + length);
        }
    }

    default void estimatePushdownFilterCost(
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext context,
            final JobScheduler jobScheduler,
            final LongConsumer onComplete,
            final Consumer<Exception> onError) {
        // Default to having no benefit by pushing down.
        onComplete.accept(Long.MAX_VALUE);
    }

    default void pushdownFilter(
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext context,
            final long costCeiling,
            final JobScheduler jobScheduler,
            final Consumer<PushdownResult> onComplete,
            final Consumer<Exception> onError) {
        // Default to returning all results as "maybe"
        onComplete.accept(PushdownResult.allMaybeMatch(selection));
    }

    default PushdownFilterContext makePushdownFilterContext(
            final WhereFilter filter,
            final List<ColumnSource<?>> filterSources) {
        return PushdownFilterContext.NO_PUSHDOWN_CONTEXT;
    }
}
