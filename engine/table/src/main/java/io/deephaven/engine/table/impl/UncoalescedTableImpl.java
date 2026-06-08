//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.Liveness;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The standard implementation of an UncoalescedTable.
 *
 * <p>
 * When the Table is {@link #coalesce() coalesced}, a hard reference is stored to the result. Any future
 * {@link #coalesce()} call reuses the same result. If the coalesce operation is context dependent, then you must
 * directly extend {@link UncoalescedTable} instead of extending {@link UncoalescedTableImpl}.
 * </p>
 *
 * @param <IMPL_TYPE> the specific type of UncoalescedTable that this class implements, used for covariant return types
 */
public abstract class UncoalescedTableImpl<IMPL_TYPE extends UncoalescedTable<IMPL_TYPE>>
        extends UncoalescedTable<IMPL_TYPE> {
    private final Object coalescingLock = new Object();

    private volatile Table coalesced;

    protected UncoalescedTableImpl(@NotNull final TableDefinition definition, @NotNull final String description) {
        super(definition, description);
    }

    /**
     * Produce the actual coalesced result table, suitable for caching.
     * <p>
     * Note that if this table must have listeners registered, etc, setting these up is the implementation's
     * responsibility.
     * <p>
     * Also note that the implementation should copy attributes, as in
     * {@code copyAttributes(resultTable, CopyAttributeOperation.Coalesce)}.
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
            synchronized (coalescingLock) {
                if (Liveness.verifyCachedObjectForReuse(localCoalesced = coalesced)) {
                    return localCoalesced;
                }
                return coalesced = doCoalesce();
            }
        }
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
