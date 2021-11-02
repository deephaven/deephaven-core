package io.deephaven.engine.v2;

import io.deephaven.engine.tables.live.LiveTable;
import io.deephaven.engine.tables.live.LiveTableMonitor;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.utils.*;
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
public class UpdatableTable extends QueryTable implements LiveTable {

    /**
     * Interface provided to updater functions that allows rowSet changes to be recorded for propagation.
     */
    public interface IndexChangeRecorder {

        /**
         * Flag key as an addition (or a modification if previously removed in this cycle). Must only be called in an
         * updater function.
         *
         * @param key The key
         */
        void addIndex(long key);

        /**
         * Flag key as a removal (if it wasn't added on this cycle). Must only be called in an updater function.
         *
         * @param key The key
         */
        void removeIndex(long key);

        /**
         * Flag key as an modification (unless it was added this cycle). Must only be called in an updater function.
         *
         * @param key The key
         */
        void modifyIndex(long key);
    }

    /**
     * Updater function interface.
     */
    @FunctionalInterface
    public interface Updater extends Consumer<IndexChangeRecorder> {
    }

    private final Updater updater;

    private final IndexChangeRecorder indexChangeRecorder = new IndexChangeRecorderImpl();

    private final TLongSet addedSet =
            new TLongHashSet(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, NULL_LONG);
    private final TLongSet removedSet =
            new TLongHashSet(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, NULL_LONG);
    private final TLongSet modifiedSet =
            new TLongHashSet(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, NULL_LONG);

    public UpdatableTable(@NotNull final RowSet rowSet,
            @NotNull final Map<String, ? extends ColumnSource<?>> nameToColumnSource,
            @NotNull final Updater updater) {
        super(rowSet, nameToColumnSource);
        this.updater = updater;
    }

    private class IndexChangeRecorderImpl implements IndexChangeRecorder {

        @Override
        public void addIndex(final long key) {
            // if a key is removed and then added back before a refresh, it looks like it was modified
            if (removedSet.remove(key)) {
                modifiedSet.add(key);
            } else {
                addedSet.add(key);
            }
        }

        @Override
        public void removeIndex(final long key) {
            if (getRowSet().find(key) >= 0) {
                removedSet.add(key);
            }
            // A removed key cannot be added or modified
            addedSet.remove(key);
            modifiedSet.remove(key);
        }

        @Override
        public void modifyIndex(final long key) {
            // if a key is added and then modified immediately, leave it as added
            if (!addedSet.contains(key)) {
                modifiedSet.add(key);
            }
        }
    }

    private static RowSet setToIndex(@NotNull final TLongSet set) {
        final RowSetBuilderRandom builder = RowSetFactoryImpl.INSTANCE.getRandomBuilder();
        set.forEach(key -> {
            builder.addKey(key);
            return true;
        });
        set.clear();
        return builder.build();
    }

    @Override
    public void refresh() {
        updater.accept(indexChangeRecorder);

        final RowSet added = setToIndex(addedSet);
        final RowSet removed = setToIndex(removedSet);
        final RowSet modified = setToIndex(modifiedSet);
        getRowSet().asMutable().update(added, removed);
        if (added.isNonempty() || removed.isNonempty() || modified.isNonempty()) {
            final Listener.Update update = new Listener.Update();
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

    protected void doNotifyListeners(Listener.Update update) {
        notifyListeners(update);
    }

    @Override
    public void destroy() {
        super.destroy();
        LiveTableMonitor.DEFAULT.removeTable(this);
    }
}
