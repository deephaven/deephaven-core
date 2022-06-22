/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.javascript.proto.dhinternal.flatbuffers;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(isNative = true, name = "dhinternal.flatbuffers.Encoding", namespace = JsPackage.GLOBAL)
public class Encoding {
    public static int UTF16_STRING,
            UTF8_BYTES;
}
