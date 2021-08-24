package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb_service;

import elemental2.core.Function;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb_service.ResponseStream",
    namespace = JsPackage.GLOBAL)
public interface ResponseStream<T> {
    void cancel();

    ResponseStream on(String type, Function handler);
}
