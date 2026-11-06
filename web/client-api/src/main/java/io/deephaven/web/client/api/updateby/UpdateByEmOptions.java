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

    io.deephaven.proto.backplane.grpc.UpdateByEmOptions.Builder toProto() {
        io.deephaven.proto.backplane.grpc.UpdateByEmOptions.Builder b =
                io.deephaven.proto.backplane.grpc.UpdateByEmOptions.newBuilder();
        if (onNullValue != null) {
            b.setOnNullValue(io.deephaven.proto.backplane.grpc.BadDataBehavior
                    .valueOf(onNullValue.toString()));
        }
        if (onNaNValue != null) {
            b.setOnNanValue(io.deephaven.proto.backplane.grpc.BadDataBehavior
                    .valueOf(onNaNValue.toString()));
        }
        if (onNullTime != null) {
            b.setOnNullTime(io.deephaven.proto.backplane.grpc.BadDataBehavior
                    .valueOf(onNullTime.toString()));
        }
        if (onNegativeDeltaTime != null) {
            b.setOnNegativeDeltaTime(io.deephaven.proto.backplane.grpc.BadDataBehavior
                    .valueOf(onNegativeDeltaTime.toString()));
        }
        if (onZeroDeltaTime != null) {
            b.setOnZeroDeltaTime(io.deephaven.proto.backplane.grpc.BadDataBehavior
                    .valueOf(onZeroDeltaTime.toString()));
        }
        if (bigValueContext != null) {
            b.setBigValueContext(bigValueContext.toProto());
        }
        return b;
    }
}
