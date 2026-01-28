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
        name = "dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceMetaRequest",
        namespace = JsPackage.GLOBAL)
public class RemoteFileSourceMetaRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static RemoteFileSourceMetaRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getResourceName();

        @JsProperty
        void setResourceName(String resourceName);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static RemoteFileSourceMetaRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getResourceName();

        @JsProperty
        void setResourceName(String resourceName);
    }

    public static native RemoteFileSourceMetaRequest deserializeBinary(Uint8Array bytes);

    public static native RemoteFileSourceMetaRequest deserializeBinaryFromReader(
            RemoteFileSourceMetaRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            RemoteFileSourceMetaRequest message, Object writer);

    public static native RemoteFileSourceMetaRequest.ToObjectReturnType toObject(
            boolean includeInstance, RemoteFileSourceMetaRequest msg);

    public native String getResourceName();

    public native Uint8Array serializeBinary();

    public native void setResourceName(String value);

    public native RemoteFileSourceMetaRequest.ToObjectReturnType0 toObject();

    public native RemoteFileSourceMetaRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
