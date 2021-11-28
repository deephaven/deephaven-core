package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.table.ColumnSource;
import gnu.trove.impl.Constants;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import org.jetbrains.annotations.NotNull;

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

    private final TLongSet addedSet =
            new TLongHashSet(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, NULL_LONG);
    private final TLongSet removedSet =
            new TLongHashSet(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, NULL_LONG);
    private final TLongSet modifiedSet =
            new TLongHashSet(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, NULL_LONG);

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
            if (removedSet.remove(key)) {
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
            addedSet.remove(key);
            modifiedSet.remove(key);
        }

        @Override
        public void modifyRowKey(final long key) {
            // if a key is added and then modified immediately, leave it as added
            if (!addedSet.contains(key)) {
                modifiedSet.add(key);
            }
        }
    }

    private static RowSet setToIndex(@NotNull final TLongSet set) {
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        set.forEach(key -> {
            builder.addKey(key);
            return true;
        });
        set.clear();
        return builder.build();
    }

    @Override
    public void run() {
        updater.accept(rowSetChangeRecorder);

        final RowSet added = setToIndex(addedSet);
        final RowSet removed = setToIndex(removedSet);
        final RowSet modified = setToIndex(modifiedSet);
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

    @Override
    public void destroy() {
        super.destroy();
        UpdateGraphProcessor.DEFAULT.removeSource(this);
    }
}
