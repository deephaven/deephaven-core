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
            builder().updateType(IcebergUpdateType.AUTO_REFRESHING).build();
    private static final IcebergUpdateMode MANUAL_REFRESHING =
            builder().updateType(IcebergUpdateType.MANUAL_REFRESHING).build();

    public enum IcebergUpdateType {
        STATIC, AUTO_REFRESHING, MANUAL_REFRESHING
    }

    public static Builder builder() {
        return ImmutableIcebergUpdateMode.builder();
    }

    public static IcebergUpdateMode staticMode() {
        return STATIC;
    }

    public static IcebergUpdateMode manualRefreshingMode() {
        return MANUAL_REFRESHING;
    }

    public static IcebergUpdateMode autoRefreshingMode() {
        return AUTO_REFRESHING;
    }

    public static IcebergUpdateMode autoRefreshingMode(final long refreshMs) {
        return ImmutableIcebergUpdateMode.builder()
                .updateType(IcebergUpdateType.AUTO_REFRESHING)
                .autoRefreshMs(refreshMs)
                .build();
    }

    @Value.Default
    public IcebergUpdateType updateType() {
        return IcebergUpdateType.STATIC;
    }

    @Value.Default
    public long autoRefreshMs() {
        return REFRESH_INTERVAL_MS;
    }

    public interface Builder {
        @SuppressWarnings("unused")
        Builder updateType(IcebergUpdateType updateType);

        @SuppressWarnings("unused")
        Builder autoRefreshMs(long autoRefreshMs);

        IcebergUpdateMode build();
    }
}
