package io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb_service;

import elemental2.core.Function;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.protocol.Flight_pb_service.ResponseStream",
    namespace = JsPackage.GLOBAL)
public interface ResponseStream<T> {
    void cancel();

    ResponseStream on(String type, Function handler);
}
