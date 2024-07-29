//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;

@Value.Immutable
@BuildableStyle
public abstract class IcebergUpdateMode {
    public static final IcebergUpdateMode STATIC = builder()
            .updateType(IcebergUpdateType.STATIC).build();
    @SuppressWarnings("unused")
    public static final IcebergUpdateMode AUTO_REFRESHING = builder()
            .updateType(IcebergUpdateType.AUTO_REFRESHING).build();
    @SuppressWarnings("unused")
    public static final IcebergUpdateMode MANUAL_REFRESHING = builder()
            .updateType(IcebergUpdateType.MANUAL_REFRESHING).build();

    public enum IcebergUpdateType {
        STATIC, AUTO_REFRESHING, MANUAL_REFRESHING
    }

    public static Builder builder() {
        return ImmutableIcebergUpdateMode.builder();
    }

    @SuppressWarnings("unused")
    public static IcebergUpdateMode autoRefreshing(final long refreshMs) {
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
        return 60_000L;
    }

    public interface Builder {
        @SuppressWarnings("unused")
        Builder updateType(IcebergUpdateType updateType);

        @SuppressWarnings("unused")
        Builder autoRefreshMs(long autoRefreshMs);

        IcebergUpdateMode build();
    }
}
