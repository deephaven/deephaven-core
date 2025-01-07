//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.createinputtablerequest.inputtablekind;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.CreateInputTableRequest.InputTableKind.InMemoryAppendOnly",
        namespace = JsPackage.GLOBAL)
public class InMemoryAppendOnly {
    public static native InMemoryAppendOnly deserializeBinary(Uint8Array bytes);

    public static native InMemoryAppendOnly deserializeBinaryFromReader(
            InMemoryAppendOnly message, Object reader);

    public static native void serializeBinaryToWriter(InMemoryAppendOnly message, Object writer);

    public static native Object toObject(boolean includeInstance, InMemoryAppendOnly msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
