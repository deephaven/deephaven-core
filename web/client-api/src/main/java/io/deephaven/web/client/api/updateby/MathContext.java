//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

/**
 * The precision and rounding mode for BigDecimal arithmetic on the server, matching Java's
 * {@code java.math.MathContext}. Used by {@link UpdateByControl#mathContext} and
 * {@link UpdateByEmOptions#bigValueContext}.
 */
@JsType(namespace = "dh.updateby")
public class MathContext {
    /**
     * The number of significant digits. Zero means unlimited. Must not be negative.
     */
    public int precision;
    /**
     * How to round a result to {@link #precision}. Each value matches the Java {@code java.math.RoundingMode} constant
     * of the same name. Required.
     */
    public RoundingMode roundingMode;

    @JsIgnore
    public static io.deephaven.proto.backplane.grpc.MathContext.Builder toProto(MathContext context) {
        return io.deephaven.proto.backplane.grpc.MathContext.newBuilder()
                .setPrecision(context.precision)
                .setRoundingMode(io.deephaven.proto.backplane.grpc.MathContext.RoundingMode
                        .valueOf(context.roundingMode.toString()));
    }
}
