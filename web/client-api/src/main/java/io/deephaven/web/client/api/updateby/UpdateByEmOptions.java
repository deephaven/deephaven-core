//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Options for the exponential moving operations. Each one is optional; the server's default applies to any not set.
 */
@JsType(namespace = "dh.updateby")
public class UpdateByEmOptions {
    @JsNullable
    public BadDataBehavior onNullValue;
    @JsNullable
    public BadDataBehavior onNaNValue;
    @JsNullable
    public BadDataBehavior onNullTime;
    @JsNullable
    public BadDataBehavior onNegativeDeltaTime;
    @JsNullable
    public BadDataBehavior onZeroDeltaTime;

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
