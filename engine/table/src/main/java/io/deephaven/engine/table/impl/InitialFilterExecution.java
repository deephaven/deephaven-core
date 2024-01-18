package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.impl.util.OperationInitializerJobScheduler;

/**
 * A FilterExecution that is used for initial filters. When we split off sub filters as child jobs, they are enqueued in
 * the {@link io.deephaven.engine.updategraph.OperationInitializer OperationInitializer}.
 */
class InitialFilterExecution extends AbstractFilterExecution {
    private final boolean permitParallelization;
    private final int segmentCount;

    private final JobScheduler jobScheduler;

    InitialFilterExecution(
            final QueryTable sourceTable,
            final WhereFilter[] filters,
            final RowSet addedInput,
            final boolean usePrev) {
        super(sourceTable, filters, addedInput, null, usePrev, false, ModifiedColumnSet.ALL);
        permitParallelization = permitParallelization(filters);
        segmentCount = QueryTable.PARALLEL_WHERE_SEGMENTS <= 0
                ? ExecutionContext.getContext().getOperationInitializer().parallelismFactor()
                : QueryTable.PARALLEL_WHERE_SEGMENTS;

        // If any of the filters can be parallelized, we will use the OperationInitializerJobScheduler.
        if (permitParallelization
                && ExecutionContext.getContext().getOperationInitializer().canParallelize()) {
            jobScheduler = new OperationInitializerJobScheduler();
        } else {
            jobScheduler = ImmediateJobScheduler.INSTANCE;
        }
    }

    @Override
    int getTargetSegments() {
        return segmentCount;
    }

    @Override
    boolean shouldParallelizeFilter(WhereFilter filter, long numberOfRows) {
        return permitParallelization
                && filter.permitParallelization()
                && !QueryTable.DISABLE_PARALLEL_WHERE && numberOfRows != 0
                && (QueryTable.FORCE_PARALLEL_WHERE || numberOfRows / 2 > QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT);
    }

    @Override
    JobScheduler jobScheduler() {
        return jobScheduler;
    }

    BasePerformanceEntry getBasePerformanceEntry() {
        return basePerformanceEntry;
    }
}
