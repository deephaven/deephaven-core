//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.ColumnSource;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.Map;
import java.util.function.Consumer;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * A live table that allows add, remove, and modify.
 */
public class UpdatableTable extends QueryTable implements Runnable {

    /**
     * Interface provided to updater functions that allows RowSet changes to be recorded for propagation.
     */
    public interface RowSetChangeRecorder {

        /**
         * Flag key as an addition (or a modification if previously removed in this cycle). Must only be called in an
         * updater function.
         *
         * @param key The key
         */
        void addRowKey(long key);

        /**
         * Flag key as a removal (if it wasn't added on this cycle). Must only be called in an updater function.
         *
         * @param key The key
         */
        void removeRowKey(long key);

        /**
         * Flag key as an modification (unless it was added this cycle). Must only be called in an updater function.
         *
         * @param key The key
         */
        void modifyRowKey(long key);
    }

    /**
     * Updater function interface.
     */
    @FunctionalInterface
    public interface Updater extends Consumer<RowSetChangeRecorder> {
    }

    private final Updater updater;

    private final RowSetChangeRecorder rowSetChangeRecorder = new RowSetChangeRecorderImpl();

    // Preserve the Trove default capacity (10) and load factor (0.5f) rather than fastutil's 16/0.75f. fastutil sets
    // don't have a "no entry value" concept (add/remove report success via boolean), so the Trove NULL_LONG sentinel
    // argument has no analog and is simply dropped.
    private final LongSet addedSet = new LongOpenHashSet(10, 0.5f);
    private final LongSet removedSet = new LongOpenHashSet(10, 0.5f);
    private final LongSet modifiedSet = new LongOpenHashSet(10, 0.5f);

    public UpdatableTable(@NotNull final TrackingRowSet rowSet,
            @NotNull final Map<String, ? extends ColumnSource<?>> nameToColumnSource,
            @NotNull final Updater updater) {
        super(rowSet, nameToColumnSource);
        this.updater = updater;
    }

    private class RowSetChangeRecorderImpl implements RowSetChangeRecorder {

        @Override
        public void addRowKey(final long key) {
            // if a key is removed and then added back before a run, it looks like it was modified
            if (removedSet.rem(key)) {
                modifiedSet.add(key);
            } else {
                addedSet.add(key);
            }
        }

        @Override
        public void removeRowKey(final long key) {
            if (getRowSet().find(key) >= 0) {
                removedSet.add(key);
            }
            // A removed key cannot be added or modified
            addedSet.rem(key);
            modifiedSet.rem(key);
        }

        @Override
        public void modifyRowKey(final long key) {
            // if a key is added and then modified immediately, leave it as added
            if (!addedSet.contains(key)) {
                modifiedSet.add(key);
            }
        }
    }

    private static RowSet setToRowSet(@NotNull final LongSet set) {
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        set.forEach((long key) -> builder.addKey(key));
        set.clear();
        return builder.build();
    }

    @Override
    public void run() {
        updater.accept(rowSetChangeRecorder);

        final RowSet added = setToRowSet(addedSet);
        final RowSet removed = setToRowSet(removedSet);
        final RowSet modified = setToRowSet(modifiedSet);
        getRowSet().writableCast().update(added, removed);
        if (added.isNonempty() || removed.isNonempty() || modified.isNonempty()) {
            final TableUpdateImpl update = new TableUpdateImpl();
            update.added = added;
            update.removed = removed;
            update.modified = modified;
            update.shifted = RowSetShiftData.EMPTY;
            if (modified.isNonempty()) {
                update.modifiedColumnSet = ModifiedColumnSet.ALL;
            } else {
                update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            }
            doNotifyListeners(update);
        }
    }

    protected void doNotifyListeners(TableUpdate update) {
        notifyListeners(update);
    }

    @OverridingMethodsMustInvokeSuper
    @Override
    public void destroy() {
        super.destroy();
        updateGraph.removeSource(this);
    }
}
