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
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

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

    /**
     * Serializes coalescing among threads that may wait for one another.
     */
    private final ReentrantLock coalescingLock = new ReentrantLock();

    /**
     * Guards publication of {@link #coalesced}; never held while coalescing.
     */
    private final Object publishLock = new Object();

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
     * This may be invoked concurrently on more than one thread: a thread that holds the update graph lock, or processes
     * its updates, coalesces on its own rather than wait for a coalesce in progress on another thread. Only the first
     * result is kept; the liveness referents produced by any other invocation are released.
     *
     * @return The coalesced result table, suitable for caching
     */
    protected abstract Table doCoalesce();

    @Override
    public final Table coalesce() {
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(updateGraph).open()) {
            Table localCoalesced;
            if (Liveness.verifyCachedObjectForReuse(localCoalesced = coalesced)) {
                return localCoalesced;
            }
            // A coalesce in progress may be waiting for the update graph lock, so a thread that holds the lock, or that
            // processes updates, must not wait for it.
            if (updateGraph.exclusiveLock().isHeldByCurrentThread()
                    || updateGraph.sharedLock().isHeldByCurrentThread()
                    || updateGraph.currentThreadProcessesUpdates()) {
                if (!coalescingLock.tryLock()) {
                    return coalesceAndPublish();
                }
            } else {
                coalescingLock.lock();
            }
            try {
                if (Liveness.verifyCachedObjectForReuse(localCoalesced = coalesced)) {
                    return localCoalesced;
                }
                return coalesceAndPublish();
            } finally {
                coalescingLock.unlock();
            }
        }
    }

    /**
     * Coalesce, and publish the result unless another coalesce has already published a live result, in which case that
     * result is returned and the liveness referents produced by this coalesce are released.
     *
     * @return The published coalesced result
     */
    private Table coalesceAndPublish() {
        final LivenessManager callerManager = LivenessScopeStack.peek();
        final LivenessScope coalesceScope = new LivenessScope();
        final Table result;
        try (final SafeCloseable ignored = LivenessScopeStack.open(coalesceScope, false)) {
            result = doCoalesce();
        } catch (final RuntimeException | Error failure) {
            coalesceScope.release();
            throw failure;
        }
        final Table published;
        synchronized (publishLock) {
            final Table existing = coalesced;
            if (Liveness.verifyCachedObjectForReuse(existing)) {
                published = existing;
            } else {
                published = coalesced = result;
            }
        }
        if (published == result) {
            coalesceScope.transferTo(callerManager);
        }
        coalesceScope.release();
        return published;
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
        synchronized (publishLock) {
            Assert.eqNull(this.coalesced, "this.coalesced");
            this.coalesced = coalesced;
        }
    }

    protected @Nullable final Table getCoalesced() {
        return coalesced;
    }
}
