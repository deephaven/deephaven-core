package io.deephaven.queryutil.dataadapter.locking;

import io.deephaven.engine.table.impl.NotificationStepSource;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.util.FunctionalInterfaces;
import org.jetbrains.annotations.Nullable;

/**
 * Type of locking used when retrieving data.
 */
public enum GetDataLockType {
    /**
     * Assert that the UGP lock is already held.
     */
    UGP_LOCK_ALREADY_HELD,
    /**
     * Acquire the UGP exclusive (write) lock.
     */
    UGP_EXCLUSIVE_LOCK,
    /**
     * Acquire a UGP shared (read) lock.
     */
    UGP_SHARED_LOCK,
    /**
     * Use the (usually) lock-free snapshotting mechanism.
     */
    SNAPSHOT;

    /**
     * Returns a {@code ThrowingConsumer} that takes a {@link QueryDataRetrievalOperation}, acquires a
     * {@link UpdateGraphProcessor} lock based on the specified {@code lockType}, then executes the operation with the
     * appropriate value for usePrev.
     *
     * @param lockType The way of acquiring the {@code UpdateGraphProcessor} lock.
     * @param sources Notification sources to check when using {@link #SNAPSHOT}. If sources is {@code null} or empty,
     *        then the SnapshotControl will not be notification aware. If all sources are non-refreshing
     *        {@link DynamicNode DynamicNodes}, then a non-refreshing SnapshotControl is created.
     * @return A function that runs an operation under the specified lock type.
     */
    @SuppressWarnings("WeakerAccess")
    public static FunctionalInterfaces.ThrowingBiConsumer<QueryDataRetrievalOperation, String, RuntimeException> getDoLockedConsumer(
            final GetDataLockType lockType, final @Nullable NotificationStepSource... sources) {
        switch (lockType) {
            case UGP_LOCK_ALREADY_HELD:
                return (queryDataRetrievalOperation, description) -> {
                    if (!UpdateGraphProcessor.DEFAULT.sharedLock().isHeldByCurrentThread()
                            && !UpdateGraphProcessor.DEFAULT.exclusiveLock().isHeldByCurrentThread()) {
                        throw new IllegalStateException("No UGP lock is held");
                    }

                    queryDataRetrievalOperation.retrieveData(false);
                };
            case UGP_EXCLUSIVE_LOCK:
                return (queryDataRetrievalOperation, description) -> UpdateGraphProcessor.DEFAULT.exclusiveLock()
                        .doLocked(() -> queryDataRetrievalOperation.retrieveData(false));
            case UGP_SHARED_LOCK:
                return (queryDataRetrievalOperation, description) -> UpdateGraphProcessor.DEFAULT.sharedLock()
                        .doLocked(() -> queryDataRetrievalOperation.retrieveData(false));
            case SNAPSHOT:
                if (sources == null || sources.length == 0) {
                    // not providing sources is probably a bug -- can't ensure snapshot consistency if we don't know
                    // what we were snapshotting.
                    throw new IllegalArgumentException(
                            "Snapshot notification sources must be provided when using mode SNAPSHOT!");
                }

                boolean isRefreshing = false;
                for (NotificationStepSource source : sources) {
                    final boolean sourceIsNonRefreshingNode =
                            source instanceof DynamicNode && !((DynamicNode) source).isRefreshing();
                    isRefreshing |= sourceIsNonRefreshingNode;
                }

                final boolean notificationAware = false;
                final ConstructSnapshot.SnapshotControl snapshotControl =
                        ConstructSnapshot.makeSnapshotControl(notificationAware, isRefreshing, sources);
                return (queryDataRetrievalOperation, description) -> ConstructSnapshot.callDataSnapshotFunction(
                        description, snapshotControl,
                        (usePrev, beforeClockValue) -> queryDataRetrievalOperation.retrieveData(usePrev));
            default:
                throw new UnsupportedOperationException("Unsupported lockType: " + lockType);
        }
    }
}
