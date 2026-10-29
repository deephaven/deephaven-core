//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.Liveness;
import io.deephaven.engine.liveness.LivenessManager;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.UpdateGraphWaitForbiddenException;
import io.deephaven.engine.updategraph.UpdateGraphWaitGuard;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Optional;

/**
 * The standard implementation of an UncoalescedTable.
 *
 * <p>
 * When the Table is {@link #coalesce() coalesced}, a hard reference is stored to the result. Any future
 * {@link #coalesce()} call reuses the same result if it is static or remains live. If the coalesce operation is context
 * dependent, then you must directly extend {@link UncoalescedTable} instead of extending {@link UncoalescedTableImpl}.
 * </p>
 *
 * @param <IMPL_TYPE> the specific type of UncoalescedTable that this class implements, used for covariant return types
 */
public abstract class UncoalescedTableImpl<IMPL_TYPE extends UncoalescedTable<IMPL_TYPE>>
        extends UncoalescedTable<IMPL_TYPE> {

    private final Object coalescingLock = new Object();

    private volatile Table coalesced;

    protected UncoalescedTableImpl(@NotNull final TableDefinition definition, @NotNull final String description) {
        this(definition, description, null);
    }

    /**
     * @param definition The definition for this table
     * @param description A description of this table
     * @param attributes The attributes map to use, or else {@code null} to allocate a new one
     */
    protected UncoalescedTableImpl(
            @NotNull final TableDefinition definition,
            @NotNull final String description,
            @Nullable final Map<String, Object> attributes) {
        super(definition, description, attributes);
    }

    /**
     * Produce the actual coalesced result table, suitable for caching.
     * <p>
     * Note that if this table must have listeners registered, etc, setting these up is the implementation's
     * responsibility.
     * <p>
     * Also note that the implementation should copy attributes, as in
     * {@code copyAttributes(resultTable, CopyAttributeOperation.Coalesce)}.
     * <p>
     * This is never invoked concurrently for the same table, and is invoked with an {@link OperationInitializer} that
     * cannot parallelize: initialization threads may be waiting for the coalescing lock that the invoking thread holds,
     * so it must not wait for work on those threads. For a refreshing table, a thread that may wait for its update
     * graph first invokes this within its own {@link LivenessScope} while {@link UpdateGraphWaitGuard#forbidWaits()
     * forbidden} to wait for any update graph. If the attempt succeeds, the liveness referents it produced are
     * transferred to the caller's liveness manager. If it would wait, it is abandoned, the liveness referents it
     * produced are released, and this is invoked again while the thread holds the shared lock.
     *
     * @return The coalesced result table, suitable for caching
     */
    protected abstract Table doCoalesce();

    @Override
    public final Table coalesce() {
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(updateGraph)
                .withOperationInitializer(OperationInitializer.NON_PARALLELIZABLE).open()) {
            Table localCoalesced;
            if (Liveness.verifyCachedObjectForReuse(localCoalesced = coalesced)) {
                return localCoalesced;
            }
            if (!isRefreshing() || currentThreadMustNotWaitForOtherThreads()) {
                // the holder of the coalescing lock never waits for the update graph, so this thread may wait for it
                synchronized (coalescingLock) {
                    if (Liveness.verifyCachedObjectForReuse(localCoalesced = coalesced)) {
                        return localCoalesced;
                    }
                    return coalesced = doCoalesce();
                }
            }
            synchronized (coalescingLock) {
                if (Liveness.verifyCachedObjectForReuse(localCoalesced = coalesced)) {
                    return localCoalesced;
                }
                localCoalesced = coalesceWithoutWaitingForUpdateGraph();
                if (localCoalesced != null) {
                    return coalesced = localCoalesced;
                }
            }
            return updateGraph.sharedLock().computeLocked(this::coalesce);
        }
    }

    /**
     * Attempt to coalesce while {@link UpdateGraphWaitGuard#forbidWaits() forbidden} to wait for any update graph, in a
     * {@link LivenessScope} whose referents are transferred to the caller's liveness manager if the attempt succeeds.
     *
     * @return The coalesced result, or {@code null} if the attempt would have waited for an update graph
     */
    @Nullable
    private Table coalesceWithoutWaitingForUpdateGraph() {
        final LivenessManager callerManager = LivenessScopeStack.peek();
        final LivenessScope coalesceScope = new LivenessScope();
        try {
            final Table result;
            try (final SafeCloseable ignored1 = LivenessScopeStack.open(coalesceScope, false);
                    final SafeCloseable ignored2 = UpdateGraphWaitGuard.forbidWaits()) {
                result = doCoalesce();
            } catch (final RuntimeException failure) {
                if (ExceptionUtils.indexOfType(failure, UpdateGraphWaitForbiddenException.class) < 0) {
                    throw failure;
                }
                return null;
            }
            coalesceScope.transferTo(callerManager);
            return result;
        } finally {
            coalesceScope.release();
        }
    }

    @Override
    public final Optional<Table> coalescedIfAvailable() {
        return verifyCoalescedForReuse(coalesced);
    }

    /**
     * Proactively set the coalesced result table. See {@link #doCoalesce()} for the caller's responsibilities. Note
     * that it is an error to call this more than once with a non-null input.
     *
     * @param coalesced The coalesced result table, suitable for caching
     */
    protected final void setCoalesced(final Table coalesced) {
        synchronized (coalescingLock) {
            Assert.eqNull(this.coalesced, "this.coalesced");
            this.coalesced = coalesced;
        }
    }

    protected @Nullable final Table getCoalesced() {
        return coalesced;
    }
}
