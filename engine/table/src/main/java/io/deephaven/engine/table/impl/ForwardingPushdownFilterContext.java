//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.select.WhereFilter;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link BasePushdownFilterContextImpl} for a matcher that delegates to other matchers, each with a context of its
 * own. The child contexts receive the executed filter cost given to this context, and are closed with it.
 */
public class ForwardingPushdownFilterContext extends BasePushdownFilterContextImpl {

    private final List<PushdownFilterContext> childContexts = new ArrayList<>();

    public ForwardingPushdownFilterContext(
            final WhereFilter filter,
            final List<ColumnSource<?>> columnSources) {
        super(filter, columnSources);
    }

    /**
     * Register a child context.
     */
    protected final void addChildContext(final PushdownFilterContext child) {
        childContexts.add(child);
    }

    @Override
    public void updateExecutedFilterCost(final long executedFilterCost) {
        super.updateExecutedFilterCost(executedFilterCost);
        forwardExecutedFilterCost(executedFilterCost);
    }

    /**
     * Pass an executed filter cost on to the child contexts. Override to lower it for children that ran fewer steps.
     */
    protected void forwardExecutedFilterCost(final long executedFilterCost) {
        childContexts.forEach(child -> child.updateExecutedFilterCost(executedFilterCost));
    }

    @Override
    public void close() {
        childContexts.forEach(PushdownFilterContext::close);
        super.close();
    }
}
