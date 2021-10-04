/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.impl;

import com.google.flatbuffers.FlatBufferBuilder;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.barrage.flatbuf.BarrageSerializationOptions;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class BarrageSubscriptionOptions {
    public enum ColumnConversionMode {
        Stringify, JavaSerialization, ThrowError
    }

    public static Builder builder() {
        return ImmutableBarrageSubscriptionOptions.builder();
    }

    public static BarrageSubscriptionOptions of(final BarrageSerializationOptions options) {
        final byte mode = options.columnConversionMode();
        return builder()
                .useDeephavenNulls(options.useDeephavenNulls())
                .columnConversionMode(conversionModeFbToEnum(mode))
                .updateIntervalMs(options.update)
                .build();
    }

    public static BarrageSubscriptionOptions of(final BarrageSubscriptionRequest subscriptionRequest) {
        final byte mode = subscriptionRequest.serializationOptions().columnConversionMode();
        return builder()
                .useDeephavenNulls(subscriptionRequest.serializationOptions().useDeephavenNulls())
                .columnConversionMode(conversionModeFbToEnum(mode))
                .build();
    }

    /**
     * By default, prefer to communicate null values using the arrow-compatible validity structure.
     *
     * @return whether to use deephaven nulls
     */
    @Default
    public boolean useDeephavenNulls() {
        return false;
    }

    /**
     * By default, we should not specify anything; the server will use whatever it is configured with. If multiple
     * subscriptions exist on a table (via the same client or via multiple clients) then the server will re-use
     * state needed to perform barrage-acrobatics for both of them. This greatly reduces the burden each client
     * adds to the server's workload. If a given table does want a shorter interval, consider using that shorter
     * interval for all subscriptions to that table.
     *
     * The default interval can be set on the server with the flag
     * {@code io.deephaven.grpc_api.arrow.ArrowFlightUtil#DEFAULT_UPDATE_INTERVAL_MS}, or
     * {@code -Dbarrage.updateInterval=1000}.
     *
     * Related, when shortining the updateInterval, you typically want to ensure your LTM cycle is short enough
     * to update that quickly, too. This can be done on the server with the flag
     * {@code io.deephaven.db.tables.live.LiveTableMonitor#defaultTargetCycleTime}, or
     * {@code -DLiveTableMonitor.targetcycletime=1000}.
     *
     * @return the update interval to subscribe for
     */
    @Default
    public int updateIntervalMs() {
        return 0;
    }

    @Default
    public ColumnConversionMode columnConversionMode() {
        return ColumnConversionMode.Stringify;
    }

    public int appendTo(FlatBufferBuilder builder) {
        return BarrageSerializationOptions.createBarrageSerializationOptions(
                builder, conversionModeEnumToFb(columnConversionMode()), useDeephavenNulls());
    }

    private static ColumnConversionMode conversionModeFbToEnum(final byte mode) {
        switch (mode) {
            case io.deephaven.barrage.flatbuf.ColumnConversionMode.Stringify:
                return ColumnConversionMode.Stringify;
            case io.deephaven.barrage.flatbuf.ColumnConversionMode.JavaSerialization:
                return ColumnConversionMode.JavaSerialization;
            case io.deephaven.barrage.flatbuf.ColumnConversionMode.ThrowError:
                return ColumnConversionMode.ThrowError;
            default:
                throw new UnsupportedOperationException("Unexpected column conversion mode " + mode + " (byte)");
        }
    }

    private static byte conversionModeEnumToFb(final ColumnConversionMode mode) {
        switch (mode) {
            case Stringify:
                return io.deephaven.barrage.flatbuf.ColumnConversionMode.Stringify;
            case JavaSerialization:
                return io.deephaven.barrage.flatbuf.ColumnConversionMode.JavaSerialization;
            case ThrowError:
                return io.deephaven.barrage.flatbuf.ColumnConversionMode.ThrowError;
            default:
                throw new UnsupportedOperationException("Unexpected column conversion mode " + mode + " (enum)");
        }
    }

    public interface Builder {

        Builder useDeephavenNulls(boolean useDeephavenNulls);

        Builder columnConversionMode(ColumnConversionMode columnConversionMode);

        Builder updateIntervalMs(int updateIntervalMs);

        BarrageSubscriptionOptions build();
    }
}
