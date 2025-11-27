//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceSetConnectionIdRequest",
        namespace = JsPackage.GLOBAL)
public class RemoteFileSourceSetConnectionIdRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static RemoteFileSourceSetConnectionIdRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getConnectionId();

        @JsProperty
        void setConnectionId(String connectionId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static RemoteFileSourceSetConnectionIdRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getConnectionId();

        @JsProperty
        void setConnectionId(String connectionId);
    }

    public static native RemoteFileSourceSetConnectionIdRequest deserializeBinary(Uint8Array bytes);

    public static native RemoteFileSourceSetConnectionIdRequest deserializeBinaryFromReader(
            RemoteFileSourceSetConnectionIdRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            RemoteFileSourceSetConnectionIdRequest message, Object writer);

    public static native RemoteFileSourceSetConnectionIdRequest.ToObjectReturnType toObject(
            boolean includeInstance, RemoteFileSourceSetConnectionIdRequest msg);

    public native String getConnectionId();

    public native Uint8Array serializeBinary();

    public native void setConnectionId(String value);

    public native RemoteFileSourceSetConnectionIdRequest.ToObjectReturnType0 toObject();

    public native RemoteFileSourceSetConnectionIdRequest.ToObjectReturnType0 toObject(
            boolean includeInstance);
}
