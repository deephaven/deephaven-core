//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.remotefilesource_pb.SetExecutionContextRequest",
        namespace = JsPackage.GLOBAL)
public class SetExecutionContextRequest {
    public static native SetExecutionContextRequest deserializeBinary(Uint8Array bytes);

    public static native SetExecutionContextRequest deserializeBinaryFromReader(
            SetExecutionContextRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            SetExecutionContextRequest message, Object writer);

    public native void clearTopLevelPackagesList();

    public native JsArray<String> getTopLevelPackagesList();

    public native Uint8Array serializeBinary();


    public native void setTopLevelPackagesList(JsArray<String> value);

    public native void addTopLevelPackages(String value);

    public native void addTopLevelPackages(String value, double index);
}

