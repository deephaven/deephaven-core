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
        name = "dhinternal.io.deephaven_core.proto.storage_pb.FetchFileRequest",
        namespace = JsPackage.GLOBAL)
public class FetchFileRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static FetchFileRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getEtag();

        @JsProperty
        String getPath();

        @JsProperty
        void setEtag(String etag);

        @JsProperty
        void setPath(String path);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static FetchFileRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getEtag();

        @JsProperty
        String getPath();

        @JsProperty
        void setEtag(String etag);

        @JsProperty
        void setPath(String path);
    }

    public static native FetchFileRequest deserializeBinary(Uint8Array bytes);

    public static native FetchFileRequest deserializeBinaryFromReader(
            FetchFileRequest message, Object reader);

    public static native void serializeBinaryToWriter(FetchFileRequest message, Object writer);

    public static native FetchFileRequest.ToObjectReturnType toObject(
            boolean includeInstance, FetchFileRequest msg);

    public native void clearEtag();

    public native String getEtag();

    public native String getPath();

    public native boolean hasEtag();

    public native Uint8Array serializeBinary();

    public native void setEtag(String value);

    public native void setPath(String value);

    public native FetchFileRequest.ToObjectReturnType0 toObject();

    public native FetchFileRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
