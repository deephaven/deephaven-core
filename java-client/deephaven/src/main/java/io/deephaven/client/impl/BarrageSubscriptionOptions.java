/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.impl;

import com.google.flatbuffers.FlatBufferBuilder;
import io.deephaven.barrage.flatbuf.BarrageSerializationOptions;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;

public class BarrageSubscriptionOptions {
    public enum ColumnConversionMode {
        Stringify, JavaSerialization, ThrowError
    }

    public final boolean useDeephavenNulls;
    public final ColumnConversionMode columnConversionMode;

    public static BarrageSubscriptionOptions of(final BarrageSerializationOptions options) {
        final byte mode = options.columnConversionMode();
        return new Builder()
                .setUseDeephavenNulls(options.useDeephavenNulls())
                .setColumnConversionMode(conversionModeFbToEnum(mode))
                .build();
    }

    public static BarrageSubscriptionOptions of(final BarrageSubscriptionRequest subscriptionRequest) {
        final byte mode = subscriptionRequest.serializationOptions().columnConversionMode();
        return new Builder()
                .setUseDeephavenNulls(subscriptionRequest.serializationOptions().useDeephavenNulls())
                .setColumnConversionMode(conversionModeFbToEnum(mode))
                .build();
    }

    public int appendTo(FlatBufferBuilder builder) {
        return BarrageSerializationOptions.createBarrageSerializationOptions(
                builder, conversionModeEnumToFb(columnConversionMode), useDeephavenNulls);
    }

    private static ColumnConversionMode conversionModeFbToEnum(byte mode) {
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

    private static byte conversionModeEnumToFb(ColumnConversionMode mode) {
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

    private BarrageSubscriptionOptions(
            final boolean useDeephavenNulls,
            final ColumnConversionMode columnConversionMode) {
        this.useDeephavenNulls = useDeephavenNulls;
        this.columnConversionMode = columnConversionMode;
    }

    public static class Builder {
        private boolean useDeephavenNulls;
        private ColumnConversionMode columnConversionMode = ColumnConversionMode.Stringify;

        public Builder setUseDeephavenNulls(final boolean useDeephavenNulls) {
            this.useDeephavenNulls = useDeephavenNulls;
            return this;
        }

        public Builder setColumnConversionMode(final ColumnConversionMode columnConversionMode) {
            this.columnConversionMode = columnConversionMode;
            return this;
        }

        public BarrageSubscriptionOptions build() {
            return new BarrageSubscriptionOptions(useDeephavenNulls, columnConversionMode);
        }
    }
}
