//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;

@Value.Immutable
@BuildableStyle
public abstract class IcebergUpdateMode {
    private static final long REFRESH_INTERVAL_MS = 60_000L;

    private static final IcebergUpdateMode STATIC = builder().updateType(IcebergUpdateType.STATIC).build();
    private static final IcebergUpdateMode AUTO_REFRESHING =
            builder().updateType(IcebergUpdateType.AUTO_REFRESHING).autoRefreshMs(REFRESH_INTERVAL_MS).build();
    private static final IcebergUpdateMode MANUAL_REFRESHING =
            builder().updateType(IcebergUpdateType.MANUAL_REFRESHING).build();

    public enum IcebergUpdateType {
        STATIC, AUTO_REFRESHING, MANUAL_REFRESHING
    }

    /**
     * Set a static update mode for this table.
     */
    public static IcebergUpdateMode staticMode() {
        return STATIC;
    }

    /**
     * Set a manually-refreshing update mode for this table. The ordering of the data in the table will depend on the
     * order in which the data files are discovered on refresh.
     */
    public static IcebergUpdateMode manualRefreshingMode() {
        return MANUAL_REFRESHING;
    }

    /**
     * Set an automatically-refreshing update mode for this table using the default refresh interval of 60 seconds. The
     * ordering of the data in the table will depend on the order in which the data files are discovered on refresh.
     */
    public static IcebergUpdateMode autoRefreshingMode() {
        return AUTO_REFRESHING;
    }

    /**
     * Set an automatically-refreshing update mode for this table using the provided refresh interval. The ordering of
     * the data in the table will depend on the order in which the data files are discovered on refresh.
     *
     * @param refreshMs the refresh interval in milliseconds
     */
    public static IcebergUpdateMode autoRefreshingMode(final long refreshMs) {
        return ImmutableIcebergUpdateMode.builder()
                .updateType(IcebergUpdateType.AUTO_REFRESHING)
                .autoRefreshMs(refreshMs)
                .build();
    }

    /**
     * Set the update mode for the result table. The default is {@link IcebergUpdateType#STATIC}.
     */
    @Value.Default
    public IcebergUpdateType updateType() {
        return IcebergUpdateType.STATIC;
    }

    /**
     * When the update type is {@link IcebergUpdateType#AUTO_REFRESHING}, this value specifies the minimum interval in
     * milliseconds between automatic refreshes of the table.
     */
    @Value.Default
    public long autoRefreshMs() {
        return 0;
    }


    private static Builder builder() {
        return ImmutableIcebergUpdateMode.builder();
    }

    protected interface Builder {
        @SuppressWarnings("unused")
        Builder updateType(IcebergUpdateType updateType);

        @SuppressWarnings("unused")
        Builder autoRefreshMs(long autoRefreshMs);

        IcebergUpdateMode build();
    }

    @Value.Check
    protected void checkAutoRefreshingInterval() {
        if (updateType() != IcebergUpdateType.AUTO_REFRESHING && autoRefreshMs() != 0) {
            throw new IllegalArgumentException(
                    "Auto-refresh interval must not be set when update type is not auto-refreshing");
        }
        if (updateType() == IcebergUpdateType.AUTO_REFRESHING && autoRefreshMs() <= 0) {
            throw new IllegalArgumentException("Auto-refresh interval must be positive");
        }
    }
}
