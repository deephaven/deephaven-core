//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import com.google.flatbuffers.FlatBufferBuilder;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class BarrageSubscriptionOptions implements StreamReaderOptions {

    public static Builder builder() {
        return ImmutableBarrageSubscriptionOptions.builder();
    }

    public static BarrageSubscriptionOptions of(final io.deephaven.barrage.flatbuf.BarrageSubscriptionOptions options) {
        if (options == null) {
            return builder().build();
        }
        final byte mode = options.columnConversionMode();
        return builder()
                .useDeephavenNulls(options.useDeephavenNulls())
                .columnConversionMode(ColumnConversionMode.conversionModeFbToEnum(mode))
                .minUpdateIntervalMs(options.minUpdateIntervalMs())
                .batchSize(options.batchSize())
                .maxMessageSize(options.maxMessageSize())
                .columnsAsList(options.columnsAsList())
                .build();
    }

    public static BarrageSubscriptionOptions of(final BarrageSubscriptionRequest subscriptionRequest) {
        return of(subscriptionRequest.subscriptionOptions());
    }

    /**
     * By default, prefer to communicate null values using the arrow-compatible validity structure.
     *
     * @return whether to use deephaven nulls
     */
    @Override
    @Default
    public boolean useDeephavenNulls() {
        return false;
    }

    /**
     * Requesting clients can specify whether they want columns to be returned wrapped in a list. This enables easier
     * support in some official arrow clients, but is not the default.
     */
    @Override
    @Default
    public boolean columnsAsList() {
        return false;
    }

    /**
     * By default, we should not specify anything; the server will use whatever it is configured with. If multiple
     * subscriptions exist on a table (via the same client or via multiple clients) then the server will re-use state
     * needed to perform barrage-acrobatics for both of them. This greatly reduces the burden each client adds to the
     * server's workload. If a given table does want a shorter interval, consider using that shorter interval for all
     * subscriptions to that table.
     *
     * The default interval can be set on the server with the flag
     * {@code io.deephaven.server.arrow.ArrowFlightUtil#DEFAULT_UPDATE_INTERVAL_MS}, or
     * {@code -Dbarrage.minUpdateInterval=1000}.
     *
     * Related, when shortening the minUpdateInterval, you typically want to shorten the server's UGP cycle enough to
     * update at least as quickly. This can be done on the server with the flag
     * {@code io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph#defaultTargetCycleTime}, or
     * {@code -DPeriodicUpdateGraph.targetcycletime=1000}.
     *
     * @return the update interval to subscribe for
     */
    @Default
    public int minUpdateIntervalMs() {
        return 0;
    }

    /**
     * @return the preferred batch size if specified
     */
    @Override
    @Default
    public int batchSize() {
        return 0;
    }

    /**
     * @return the preferred maximum GRPC message size if specified
     */
    @Override
    @Default
    public int maxMessageSize() {
        return 0;
    }

    @Override
    @Default
    public ColumnConversionMode columnConversionMode() {
        return ColumnConversionMode.Stringify;
    }

    public int appendTo(FlatBufferBuilder builder) {
        return io.deephaven.barrage.flatbuf.BarrageSubscriptionOptions.createBarrageSubscriptionOptions(
                builder, ColumnConversionMode.conversionModeEnumToFb(columnConversionMode()), useDeephavenNulls(),
                minUpdateIntervalMs(),
                batchSize(),
                maxMessageSize(),
                columnsAsList());
    }

    public interface Builder {

        Builder useDeephavenNulls(boolean useDeephavenNulls);

        Builder columnsAsList(boolean columnsAsList);

        Builder columnConversionMode(ColumnConversionMode columnConversionMode);

        Builder minUpdateIntervalMs(int minUpdateIntervalMs);

        Builder batchSize(int batchSize);

        Builder maxMessageSize(int messageSize);

        BarrageSubscriptionOptions build();
    }
}
