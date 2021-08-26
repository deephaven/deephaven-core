/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.modelfarm;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.db.v2.InstrumentedListenerAdapter;
import io.deephaven.db.v2.NotificationStepSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.FunctionalInterfaces;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Abstract class for ModelFarm implementations that will take data from a {@link RowDataManager}.
 * This class tracks the mappings between each key and the corresponding index in the
 * {@code RowDataManager}'s {@link RowDataManager#table() table}. Each row of this table should
 * contain all of the data necessary to populate an instance of {@code DATATYPE}, which will then be
 * passed to the {@link ModelFarmBase#model model}.
 *
 * @param <KEYTYPE> The type of the keys (e.g. {@link io.deephaven.modelfarm.fitterfarm.FitScope}).
 * @param <DATATYPE> The type of the data (e.g.
 *        {@link io.deephaven.modelfarm.fitterfarm.futures.FuturesFitDataOptionPrices}.
 * @param <ROWDATAMANAGERTYPE> The type of the RowDataManager (e.g.
 *        {@link io.deephaven.modelfarm.fitterfarm.futures.FuturesFitDataManager}).
 */
public abstract class RDMModelFarm<KEYTYPE, DATATYPE, ROWDATAMANAGERTYPE extends RowDataManager<KEYTYPE, DATATYPE>>
    extends ModelFarmBase<DATATYPE> {

    private static final Logger log = LoggerFactory.getLogger(RDMModelFarm.class);
    private static final long NO_ENTRY_VALUE = -1;
    private static final long REMOVED_ENTRY_VALUE = -2;

    @SuppressWarnings("WeakerAccess")
    protected final ROWDATAMANAGERTYPE dataManager;

    private final ReadWriteLock keyIndexCurrentLock = new ReentrantReadWriteLock();
    private final ReadWriteLock keyIndexPrevLock = new ReentrantReadWriteLock();
    private final TObjectLongMap<KEYTYPE> keyIndexPrev =
        new TObjectLongHashMap<>(10, 0.5f, NO_ENTRY_VALUE);
    private final TObjectLongMap<KEYTYPE> keyIndexDelta =
        new TObjectLongHashMap<>(10, 0.5f, NO_ENTRY_VALUE);

    // keep the listener so that it doesn't get garbage collected
    @SuppressWarnings("FieldCanBeLocal")
    private InstrumentedListenerAdapter listener = null;

    /**
     * Create a multithreaded resource to execute data driven models.
     *
     * @param nThreads number of worker threads.
     * @param model model to execute.
     * @param dataManager interface for accessing and querying data contained in rows of a dynamic
     *        table.
     */
    @SuppressWarnings("WeakerAccess")
    public RDMModelFarm(final int nThreads, final Model<DATATYPE> model,
        final ROWDATAMANAGERTYPE dataManager) {
        super(nThreads, model);
        this.dataManager = Require.neqNull(dataManager, "dataManager");
    }

    @Override
    protected void modelFarmStarted() {
        Assert.eqNull(listener, "listener");
        listener = new InstrumentedListenerAdapter(dataManager.table(), false) {
            private static final long serialVersionUID = -2137065147841887955L;

            @Override
            public void onUpdate(Index added, Index removed, Index modified) {
                keyIndexCurrentLock.writeLock().lock();
                keyIndexPrevLock.writeLock().lock();

                keyIndexDelta.forEachEntry((key, idx) -> {
                    if (idx == REMOVED_ENTRY_VALUE) {
                        keyIndexPrev.remove(key);
                    } else {
                        keyIndexPrev.put(key, idx);
                    }

                    return true;
                });

                keyIndexPrevLock.writeLock().unlock();
                keyIndexDelta.clear();

                removeKeyIndex(removed);
                removeKeyIndex(modified);
                addKeyIndex(added);
                addKeyIndex(modified);
                keyIndexCurrentLock.writeLock().unlock();
                onDataUpdate(added, removed, modified);
            }
        };

        dataManager.table().listenForUpdates(listener, true);
    }

    private void removeKeyIndex(final Index index) {
        index.forAllLongs((final long i) -> {
            final KEYTYPE key = dataManager.uniqueIdPrev(i);
            keyIndexDelta.put(key, REMOVED_ENTRY_VALUE);
        });
    }

    private void addKeyIndex(final Index index) {
        index.forAllLongs((final long i) -> {
            final KEYTYPE key = dataManager.uniqueIdCurrent(i);
            keyIndexDelta.put(key, i);
        });
    }

    /**
     * Process a change to the data table. If the data table is being accessed, use the protected
     * column source fields.
     *
     * @param added new indexes added to the data table
     * @param removed indexes removed from the data table
     * @param modified indexes modified in the data table.
     */
    protected abstract void onDataUpdate(Index added, Index removed, Index modified);

    /**
     * Populates a data object with data from the most recent row with the provided unique
     * identifier.
     *
     * @param data data structure to populate
     * @param key key to load data for
     * @param usePrev use data from the previous table update
     * @return true if the data loaded; false if there was no data to load.
     */
    private boolean loadData(final DATATYPE data, final KEYTYPE key, final boolean usePrev) {
        // if this is called in the update loop, keyIndex should be updated and accessed in the same
        // thread.
        // if this is called outside the update loop, the access should be in the LTM lock, and the
        // update should be in the update loop
        // therefore, keyIndex does not need synchronization.

        long i;

        if (usePrev) {
            keyIndexPrevLock.readLock().lock();
            i = keyIndexPrev.get(key);
            keyIndexPrevLock.readLock().unlock();
        } else {
            keyIndexCurrentLock.readLock().lock();
            i = keyIndexDelta.get(key);

            if (i == REMOVED_ENTRY_VALUE) {
                i = NO_ENTRY_VALUE;
            } else if (i == NO_ENTRY_VALUE) {
                keyIndexPrevLock.readLock().lock();
                i = keyIndexPrev.get(key);
                keyIndexPrevLock.readLock().unlock();
            }

            keyIndexCurrentLock.readLock().unlock();
        }

        if (i == NO_ENTRY_VALUE) {
            log.warn().append("Attempting to get row data for a key with no index.  key=")
                .append(key.toString()).endl();
            return false;
        }

        dataManager.loadData(data, i, usePrev);
        return true;
    }

    /**
     * Returns a function that takes a key and returns an instance of {@code DATATYPE} that contains
     * the most recent data for that key. The returned function will retrieve the data using the
     * specified {@code lockType}.
     *
     * @param lockType locking algorithm used to ensure that data read from the table is consistent.
     * @return function to retrieve the most recent row data for a unique identifier.
     */
    @SuppressWarnings("WeakerAccess")
    protected ModelFarmBase.MostRecentDataGetter<KEYTYPE, DATATYPE> getMostRecentDataFactory(
        final ModelFarmBase.GetDataLockType lockType) {
        // Get the "doLockedConsumer", which will call the FitDataPopulator (i.e. the lambda below)
        // using the configured
        // lock type and the appropriate value for 'usePrev'.
        final FunctionalInterfaces.ThrowingBiConsumer<ModelFarmBase.QueryDataRetrievalOperation, NotificationStepSource, RuntimeException> doLockedConsumer =
            getDoLockedConsumer(lockType);
        return (key) -> {
            final DATATYPE data = dataManager.newData();
            final boolean[] isOk = new boolean[1];

            // (This lambda is a FitDataPopulator.)
            doLockedConsumer.accept(usePrev -> isOk[0] = loadData(data, key, usePrev),
                (NotificationStepSource) dataManager.table());

            if (isOk[0]) {
                return data;
            } else {
                return null;
            }
        };
    }

}
