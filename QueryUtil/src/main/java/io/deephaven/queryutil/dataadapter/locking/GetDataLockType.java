package io.deephaven.queryutil.dataadapter.locking;

import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.util.FunctionalInterfaces;

/**
 * Type of locking used when retrieving data.
 */
public enum GetDataLockType {
    /**
     * The LTM lock is already held.
     */
    LTM_LOCK_ALREADY_HELD,
    /**
     * Acquire the LTM lock.
     */
    LTM_LOCK,
    /**
     * Acquire an LTM read lock.
     */
    LTM_READ_LOCK,
    /**
     * Use the (usually) lock-free snapshotting mechanism.
     */
    SNAPSHOT;

    /**
     * Returns a {@code ThrowingConsumer} that takes a {@link QueryDataRetrievalOperation}, acquires a
     * {@link UpdateGraphProcessor} lock based on the specified {@code lockType}, then executes the
     * {@code FitDataPopulator} with the appropriate value for usePrev.
     *
     * @param lockType The way of acquiring the {@code UpdateGraphProcessor} lock.
     * @return A function that runs a {@link }
     */
    @SuppressWarnings("WeakerAccess")
    public static FunctionalInterfaces.ThrowingBiConsumer<QueryDataRetrievalOperation, String, RuntimeException> getDoLockedConsumer(
            final GetDataLockType lockType) {
        switch (lockType) {
            case LTM_LOCK_ALREADY_HELD:
                return (queryDataRetrievalOperation, description) -> queryDataRetrievalOperation.retrieveData(false);
            case LTM_LOCK:
                return (queryDataRetrievalOperation, description) -> UpdateGraphProcessor.DEFAULT.exclusiveLock()
                        .doLocked(() -> queryDataRetrievalOperation.retrieveData(false));
            case LTM_READ_LOCK:
                return (queryDataRetrievalOperation, description) -> UpdateGraphProcessor.DEFAULT.sharedLock()
                        .doLocked(() -> queryDataRetrievalOperation.retrieveData(false));
            case SNAPSHOT:
                return (queryDataRetrievalOperation, description) -> ConstructSnapshot.callDataSnapshotFunction(
                        description,
                        ConstructSnapshot.makeSnapshotControl(false,
                                // TODO: should this next boolean ('refreshing') be true or false? or depend on the
                                // table?
                                false),
                        (usePrev, beforeClockValue) -> queryDataRetrievalOperation.retrieveData(usePrev));
            default:
                throw new UnsupportedOperationException("Unsupported lockType: " + lockType);
        }
    }
}
