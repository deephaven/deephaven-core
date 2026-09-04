//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import jsinterop.annotations.JsType;

@JsType(namespace = "dh.updateby")
public class UpdateByEmOptions {
    public BadDataBehavior onNullValue;
    public BadDataBehavior onNaNValue;
    public BadDataBehavior onNullTime;
    public BadDataBehavior onNegativeDeltaTime;
    public BadDataBehavior onZeroDeltaTime;

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
