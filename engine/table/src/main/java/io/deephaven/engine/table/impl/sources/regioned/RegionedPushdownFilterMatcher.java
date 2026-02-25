//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownFilterMatcher;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.select.WhereFilter;

import java.util.List;

public interface RegionedPushdownFilterMatcher extends PushdownFilterMatcher {
    /**
     * Get the list of pushdown actions supported by this matcher. Defaults to an empty list.
     */
    default List<RegionedPushdownAction> supportedActions() {
        // Default to an empty list.
        return List.of();
    }

    /**
     * Create a context for estimating the cost for the filter and filter context. This context can be used to provide
     * additional information to the {@link #estimatePushdownAction} function
     */
    default RegionedPushdownAction.EstimateContext makeEstimateContext(
            final WhereFilter filter,
            final PushdownFilterContext context) {
        return RegionedPushdownAction.DEFAULT_ESTIMATE_CONTEXT;
    }

    /**
     * Given a list of actions, estimate the cost of the next pushdown action. This is not always the first action in
     * the list because this matcher may not support every allowed action.
     */
    default long estimatePushdownAction(
            final List<RegionedPushdownAction> actions,
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext filterContext,
            final RegionedPushdownAction.EstimateContext estimateContext) {
        return Long.MAX_VALUE;
    }

    /**
     * Create a context for processing the given filter and filter context. This context can be used to provide
     * additional information to the {@link #performPushdownAction} function
     */

    default RegionedPushdownAction.ActionContext makeActionContext(
            final WhereFilter filter,
            final PushdownFilterContext context) {
        return RegionedPushdownAction.DEFAULT_ACTION_CONTEXT;
    }

    /**
     * Perform the pushdown action for the given filter and filter context.
     */
    default PushdownResult performPushdownAction(
            final RegionedPushdownAction action,
            final WhereFilter filter,
            final RowSet selection,
            final PushdownResult input,
            final boolean usePrev,
            final PushdownFilterContext filterContext,
            final RegionedPushdownAction.ActionContext actionContext) {
        // Return a copy of the input because the caller will close the input.
        return input.copy();
    }
}
