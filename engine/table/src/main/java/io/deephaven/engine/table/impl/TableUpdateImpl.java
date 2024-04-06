//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.util.RowSetShiftDataExpander;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Implementation of {@link TableUpdate}.
 */
public class TableUpdateImpl implements TableUpdate {

    public RowSet added;

    public RowSet removed;

    public RowSet modified;

    public RowSetShiftData shifted;

    public ModifiedColumnSet modifiedColumnSet;

    /**
     * Cached version of {@link #modified} with {@link #shifted} {@link RowSetShiftData#unapply(WritableRowSet)
     * unapplied}.
     */
    private volatile WritableRowSet prevModified;

    /**
     * Cached copy of a {@link RowSetShiftDataExpander} for this update.
     */
    private volatile RowSetShiftDataExpander expander;

    // Field updater for refCount, so we can avoid creating an {@link java.util.concurrent.atomic.AtomicInteger} for
    // each instance.
    private static final AtomicIntegerFieldUpdater<TableUpdateImpl> REFERENCE_COUNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(TableUpdateImpl.class, "refCount");

    // Ensure that we clean up only after all copies of the update are released.
    private volatile int refCount = 1;

    public TableUpdateImpl() {}

    public TableUpdateImpl(final RowSet added, final RowSet removed, final RowSet modified,
            final RowSetShiftData shifted,
            final ModifiedColumnSet modifiedColumnSet) {
        this.added = added;
        this.removed = removed;
        this.modified = modified;
        this.shifted = shifted;
        this.modifiedColumnSet = modifiedColumnSet;
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }

    @Override
    public TableUpdateImpl acquire() {
        if (REFERENCE_COUNT_UPDATER.incrementAndGet(this) == 1) {
            // This doubles as a memory barrier read for the writes in reset().
            Assert.eqNull(prevModified, "prevModified");
        }
        return this;
    }

    @Override
    public void release() {
        int newRefCount = REFERENCE_COUNT_UPDATER.decrementAndGet(this);
        if (newRefCount > 0) {
            return;
        }
        Assert.eqZero(newRefCount, "newRefCount");
        reset();
    }

    @Override
    public RowSet getModifiedPreShift() {
        if (shifted().empty()) {
            return modified();
        }
        WritableRowSet localPrevModified = prevModified;
        if (localPrevModified == null) {
            synchronized (this) {
                localPrevModified = prevModified;
                if (localPrevModified == null) {
                    localPrevModified = modified().copy();
                    shifted().unapply(localPrevModified);
                    // this volatile write ensures prevModified is visible only after it is shifted
                    prevModified = localPrevModified;
                }
            }
        }
        return localPrevModified;
    }

    /**
     * Get a cached {@link RowSetShiftDataExpander expander} for this TableUpdateImpl.
     *
     * @param sourceRowSet The {@link TrackingRowSet row set} to use for expansion purposes. Must match that of the
     *        table that this update was created for.
     * @return The expander
     */
    RowSetShiftDataExpander getExpander(final TrackingRowSet sourceRowSet) {
        RowSetShiftDataExpander localExpander;
        if ((localExpander = expander) == null) {
            synchronized (this) {
                if ((localExpander = expander) == null) {
                    expander = localExpander = new RowSetShiftDataExpander(this, sourceRowSet);
                }
            }
        }
        return localExpander;
    }

    public void reset() {
        if (added() != null) {
            added().close();
            added = null;
        }
        if (removed() != null) {
            removed().close();
            removed = null;
        }
        if (modified() != null) {
            modified().close();
            modified = null;
        }
        shifted = null;
        modifiedColumnSet = null;

        if (prevModified != null) {
            prevModified.close();
            // This doubles as a memory barrier write prior to the read in acquire(). It must remain last, but for
            // the expander.
            prevModified = null;
        }
        if (expander != null) {
            expander.close();
            expander = null;
        }
    }

    /**
     * Make a deep copy of a {@link TableUpdate}.
     */
    public static TableUpdateImpl copy(@NotNull final TableUpdate tableUpdate) {
        final ModifiedColumnSet oldMCS = tableUpdate.modifiedColumnSet();
        final ModifiedColumnSet newMCS;
        if (oldMCS == ModifiedColumnSet.ALL || oldMCS == ModifiedColumnSet.EMPTY) {
            newMCS = oldMCS;
        } else {
            newMCS = new ModifiedColumnSet(oldMCS);
            newMCS.setAll(oldMCS);
        }
        return copy(tableUpdate, newMCS);
    }

    /**
     * Make a deep copy of a {@link TableUpdate} with the given {@code mcs}.
     */
    public static TableUpdateImpl copy(@NotNull final TableUpdate tableUpdate, @NotNull final ModifiedColumnSet mcs) {
        return new TableUpdateImpl(
                tableUpdate.added().copy(),
                tableUpdate.removed().copy(),
                tableUpdate.modified().copy(),
                tableUpdate.shifted(),
                mcs);
    }

    @Override
    public RowSet added() {
        return added;
    }

    @Override
    public RowSet removed() {
        return removed;
    }

    @Override
    public RowSet modified() {
        return modified;
    }

    @Override
    public RowSetShiftData shifted() {
        return shifted;
    }

    @Override
    public ModifiedColumnSet modifiedColumnSet() {
        return modifiedColumnSet;
    }
}
