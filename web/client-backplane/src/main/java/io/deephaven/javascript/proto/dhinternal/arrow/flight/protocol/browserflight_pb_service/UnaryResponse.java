/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.browserflight_pb_service;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.arrow.flight.protocol.BrowserFlight_pb_service.UnaryResponse",
        namespace = JsPackage.GLOBAL)
public interface UnaryResponse {
    void cancel();
}
