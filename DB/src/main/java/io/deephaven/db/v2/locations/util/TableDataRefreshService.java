package io.deephaven.db.v2.locations.util;

import io.deephaven.configuration.Configuration;
import io.deephaven.db.v2.locations.impl.AbstractTableLocation;
import io.deephaven.db.v2.locations.impl.AbstractTableLocationProvider;
import org.jetbrains.annotations.NotNull;

/**
 * For TableDataService and related components, this allows a simple implementation for subscription support.
 */
public interface TableDataRefreshService {

    /**
     * A subscription token interface with cancellation support.
     */
    interface CancellableSubscriptionToken {

        /**
         * Cancel this subscription.
         */
        void cancel();
    }

    /**
     * Submit a one-time task to be run asynchronously.
     *
     * @param task The task to run
     */
    void submitOneTimeAsyncTask(@NotNull Runnable task);

    /**
     * Schedule refresh for an AbstractTableLocationProvider.
     *
     * @param tableLocationProvider The table location provider
     * @return A subscription token to be used for matching, which also supports cancellation
     */
    CancellableSubscriptionToken scheduleTableLocationProviderRefresh(
            @NotNull AbstractTableLocationProvider tableLocationProvider);

    /**
     * Schedule refresh for an AbstractTableLocation.
     *
     * @param tableLocation The table location
     * @return A subscription token to be used for matching, which also supports cancellation
     */
    CancellableSubscriptionToken scheduleTableLocationRefresh(@NotNull AbstractTableLocation tableLocation);

    /**
     * Get (and possibly construct) a shared instance.
     *
     * @return The shared instance
     */
    static TableDataRefreshService getSharedRefreshService() {
        return Helper.getSharedRefreshService();
    }

    /**
     * Static helper class for holding and building the shared instance.
     */
    final class Helper {

        // region Property names
        private static final String TABLE_LOCATION_REFRESH_MILLIS_PROP = "tableLocationsRefreshMillis";
        private static final String TABLE_SIZE_REFRESH_MILLIS_PROP = "tableSizeRefreshMillis";
        private static final String REFRESH_THREAD_POOL_SIZE_PROP = "refreshThreadPoolSize";
        // endregion

        // region Global properties retrieved from Configuration; used only for static TableDataRefreshService uses
        private static final String GLOBAL_TABLE_LOCATION_REFRESH_MILLIS_PROP =
                "TableDataRefreshService." + TABLE_LOCATION_REFRESH_MILLIS_PROP;
        private static final String GLOBAL_TABLE_SIZE_REFRESH_MILLIS_PROP =
                "TableDataRefreshService." + TABLE_SIZE_REFRESH_MILLIS_PROP;
        private static final String GLOBAL_REFRESH_THREAD_POOL_SIZE_PROP =
                "TableDataRefreshService." + REFRESH_THREAD_POOL_SIZE_PROP;
        // endregion

        // region Shared property default values
        private static final long DEFAULT_TABLE_LOCATION_REFRESH_MILLIS = 60_000L;
        private static final long DEFAULT_TABLE_SIZE_REFRESH_MILLIS = 30_000L;
        private static final int DEFAULT_REFRESH_THREAD_POOL_SIZE = 10;
        // endregion

        private static volatile TableDataRefreshService sharedRefreshService;

        private Helper() {}

        private static TableDataRefreshService getSharedRefreshService() {
            if (sharedRefreshService == null) {
                synchronized (Helper.class) {
                    if (sharedRefreshService == null) {
                        sharedRefreshService = new ExecutorTableDataRefreshService("Local",
                                Configuration.getInstance().getLongWithDefault(
                                        GLOBAL_TABLE_LOCATION_REFRESH_MILLIS_PROP,
                                        DEFAULT_TABLE_LOCATION_REFRESH_MILLIS),
                                Configuration.getInstance().getLongWithDefault(GLOBAL_TABLE_SIZE_REFRESH_MILLIS_PROP,
                                        DEFAULT_TABLE_SIZE_REFRESH_MILLIS),
                                Configuration.getInstance().getIntegerWithDefault(GLOBAL_REFRESH_THREAD_POOL_SIZE_PROP,
                                        DEFAULT_REFRESH_THREAD_POOL_SIZE));
                    }
                }
            }
            return sharedRefreshService;
        }
    }

    /**
     * "Null" instance, intended for unit tests and other standalone scenarios.
     */
    final class Null implements TableDataRefreshService {

        public static final TableDataRefreshService INSTANCE = new Null();

        private Null() {}

        @Override
        public void submitOneTimeAsyncTask(@NotNull final Runnable task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CancellableSubscriptionToken scheduleTableLocationProviderRefresh(
                @NotNull final AbstractTableLocationProvider tableLocationProvider) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CancellableSubscriptionToken scheduleTableLocationRefresh(
                @NotNull final AbstractTableLocation tableLocation) {
            throw new UnsupportedOperationException();
        }
    }
}
