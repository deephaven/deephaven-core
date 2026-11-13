//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Options for the exponential moving operations: {@link UpdateByEma}, {@link UpdateByEms}, {@link UpdateByEmMin},
 * {@link UpdateByEmMax} and {@link UpdateByEmStd}. Any option left unset uses the server's default.
 */
@JsType(namespace = "dh.updateby")
public class UpdateByEmOptions {
    /** What to do when an input value is null. Defaults to {@code SKIP}. */
    @JsNullable
    public BadDataBehavior onNullValue;
    /** What to do when an input value is NaN. Defaults to {@code SKIP}. */
    @JsNullable
    public BadDataBehavior onNaNValue;
    /** For time-based decay, what to do when a row's timestamp is null. Defaults to {@code SKIP}. */
    @JsNullable
    public BadDataBehavior onNullTime;
    /**
     * For time-based decay, what to do when a row's timestamp is earlier than the previous row's. Defaults to
     * {@code THROW}.
     */
    @JsNullable
    public BadDataBehavior onNegativeDeltaTime;
    /** For time-based decay, what to do when a row's timestamp equals the previous row's. */
    @JsNullable
    public BadDataBehavior onZeroDeltaTime;

    /**
     * The precision and rounding mode for the operation's arithmetic on BigDecimal and BigInteger columns. Defaults to
     * Java's {@code MathContext.DECIMAL128}, 34 digits with {@code HALF_EVEN} rounding.
     */
    @JsNullable
    public MathContext bigValueContext;

    /**
     * Static, so that a plain JS object can be passed: it has none of this class's methods.
     */
    static io.deephaven.proto.backplane.grpc.UpdateByEmOptions.Builder toProto(UpdateByEmOptions options) {
        io.deephaven.proto.backplane.grpc.UpdateByEmOptions.Builder b =
                io.deephaven.proto.backplane.grpc.UpdateByEmOptions.newBuilder();
        if (options.onNullValue != null) {
            b.setOnNullValue(io.deephaven.proto.backplane.grpc.BadDataBehavior
                    .valueOf(options.onNullValue.toString()));
        }
        if (options.onNaNValue != null) {
            b.setOnNanValue(io.deephaven.proto.backplane.grpc.BadDataBehavior
                    .valueOf(options.onNaNValue.toString()));
        }
        if (options.onNullTime != null) {
            b.setOnNullTime(io.deephaven.proto.backplane.grpc.BadDataBehavior
                    .valueOf(options.onNullTime.toString()));
        }
        if (options.onNegativeDeltaTime != null) {
            b.setOnNegativeDeltaTime(io.deephaven.proto.backplane.grpc.BadDataBehavior
                    .valueOf(options.onNegativeDeltaTime.toString()));
        }
        if (options.onZeroDeltaTime != null) {
            b.setOnZeroDeltaTime(io.deephaven.proto.backplane.grpc.BadDataBehavior
                    .valueOf(options.onZeroDeltaTime.toString()));
        }
        if (options.bigValueContext != null) {
            b.setBigValueContext(MathContext.toProto(options.bigValueContext));
        }
        return b;
    }
}
