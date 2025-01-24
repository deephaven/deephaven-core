//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.storage_pb.CreateDirectoryRequest",
        namespace = JsPackage.GLOBAL)
public class CreateDirectoryRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static CreateDirectoryRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getPath();

        @JsProperty
        void setPath(String path);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static CreateDirectoryRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getPath();

        @JsProperty
        void setPath(String path);
    }

    public static native CreateDirectoryRequest deserializeBinary(Uint8Array bytes);

    public static native CreateDirectoryRequest deserializeBinaryFromReader(
            CreateDirectoryRequest message, Object reader);

    public static native void serializeBinaryToWriter(CreateDirectoryRequest message, Object writer);

    public static native CreateDirectoryRequest.ToObjectReturnType toObject(
            boolean includeInstance, CreateDirectoryRequest msg);

    public native String getPath();

    public native Uint8Array serializeBinary();

    public native void setPath(String value);

    public native CreateDirectoryRequest.ToObjectReturnType0 toObject();

    public native CreateDirectoryRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
