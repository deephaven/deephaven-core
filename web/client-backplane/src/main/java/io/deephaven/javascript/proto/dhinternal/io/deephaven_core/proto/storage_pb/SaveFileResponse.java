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
        name = "dhinternal.io.deephaven_core.proto.storage_pb.SaveFileResponse",
        namespace = JsPackage.GLOBAL)
public class SaveFileResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static SaveFileResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getEtag();

        @JsProperty
        void setEtag(String etag);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static SaveFileResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getEtag();

        @JsProperty
        void setEtag(String etag);
    }

    public static native SaveFileResponse deserializeBinary(Uint8Array bytes);

    public static native SaveFileResponse deserializeBinaryFromReader(
            SaveFileResponse message, Object reader);

    public static native void serializeBinaryToWriter(SaveFileResponse message, Object writer);

    public static native SaveFileResponse.ToObjectReturnType toObject(
            boolean includeInstance, SaveFileResponse msg);

    public native void clearEtag();

    public native String getEtag();

    public native boolean hasEtag();

    public native Uint8Array serializeBinary();

    public native void setEtag(String value);

    public native SaveFileResponse.ToObjectReturnType0 toObject();

    public native SaveFileResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
