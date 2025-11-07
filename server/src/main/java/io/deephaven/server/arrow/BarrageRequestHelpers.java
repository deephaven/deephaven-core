//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import io.deephaven.barrage.flatbuf.BarrageSnapshotRequest;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionOptions;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.util.BarrageProtoUtil;
import org.jetbrains.annotations.Nullable;

import java.util.BitSet;

/**
 * Provides utility methods to extract viewport, column and other information from Barrage Subscription and Snapshot
 * requests.
 */
public class BarrageRequestHelpers {
    public static final int DEFAULT_MIN_UPDATE_INTERVAL_MS =
            Configuration.getInstance().getIntegerWithDefault("barrage.minUpdateInterval", 1000);

    /**
     * Helper to retrieve the viewport RowSet from a subscription request.
     */
    @Nullable
    public static RowSet getViewport(final BarrageSubscriptionRequest subscriptionRequest) {
        final boolean hasViewport = subscriptionRequest.viewportVector() != null;
        return hasViewport ? BarrageProtoUtil.toRowSet(subscriptionRequest.viewportAsByteBuffer()) : null;
    }

    /**
     * Helper to retrieve the viewport columns from a subscription request.
     */
    @Nullable
    public static BitSet getColumns(final BarrageSubscriptionRequest subscriptionRequest) {
        final boolean hasColumns = subscriptionRequest.columnsVector() != null;
        return hasColumns ? BitSet.valueOf(subscriptionRequest.columnsAsByteBuffer()) : null;
    }

    /**
     * Helper to retrieve the viewport RowSet from a snapshot request.
     */
    @Nullable
    public static RowSet getViewport(final BarrageSnapshotRequest snapshotRequest) {
        final boolean hasViewport = snapshotRequest.viewportVector() != null;
        return hasViewport
                ? BarrageProtoUtil.toRowSet(snapshotRequest.viewportAsByteBuffer())
                : null;
    }

    /**
     * Helper to retrieve the viewport columns from a snapshot request.
     */
    @Nullable
    public static BitSet getColumns(final BarrageSnapshotRequest snapshotRequest) {
        final boolean hasColumns = snapshotRequest.columnsVector() != null;
        return hasColumns ? BitSet.valueOf(snapshotRequest.columnsAsByteBuffer()) : null;
    }

    /**
     * Helper to retrieve the update interval from a subscription request.
     */
    public static long getMinUpdateIntervalMs(final BarrageSubscriptionOptions options) {
        final long minUpdateIntervalMs;
        if (options == null || options.minUpdateIntervalMs() == 0) {
            minUpdateIntervalMs = DEFAULT_MIN_UPDATE_INTERVAL_MS;
        } else {
            minUpdateIntervalMs = options.minUpdateIntervalMs();
        }
        return minUpdateIntervalMs;
    }
}
