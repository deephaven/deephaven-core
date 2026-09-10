//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh.updateby")
public class MathContext {
    public int precision;
    public RoundingMode roundingMode;

    @JsIgnore
    public io.deephaven.proto.backplane.grpc.MathContext.Builder toProto() {
        return io.deephaven.proto.backplane.grpc.MathContext.newBuilder()
                .setPrecision(precision)
                .setRoundingMode(io.deephaven.proto.backplane.grpc.MathContext.RoundingMode
                        .valueOf(roundingMode.toString()));
    }
}
