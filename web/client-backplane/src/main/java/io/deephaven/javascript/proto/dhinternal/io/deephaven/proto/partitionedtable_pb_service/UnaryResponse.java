/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.partitionedtable_pb_service;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.partitionedtable_pb_service.UnaryResponse",
        namespace = JsPackage.GLOBAL)
public interface UnaryResponse {
    void cancel();
}
