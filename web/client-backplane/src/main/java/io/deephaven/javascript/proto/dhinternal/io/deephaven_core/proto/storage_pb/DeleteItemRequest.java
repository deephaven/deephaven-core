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
        name = "dhinternal.io.deephaven_core.proto.storage_pb.DeleteItemRequest",
        namespace = JsPackage.GLOBAL)
public class DeleteItemRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static DeleteItemRequest.ToObjectReturnType create() {
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
        static DeleteItemRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getPath();

        @JsProperty
        void setPath(String path);
    }

    public static native DeleteItemRequest deserializeBinary(Uint8Array bytes);

    public static native DeleteItemRequest deserializeBinaryFromReader(
            DeleteItemRequest message, Object reader);

    public static native void serializeBinaryToWriter(DeleteItemRequest message, Object writer);

    public static native DeleteItemRequest.ToObjectReturnType toObject(
            boolean includeInstance, DeleteItemRequest msg);

    public native String getPath();

    public native Uint8Array serializeBinary();

    public native void setPath(String value);

    public native DeleteItemRequest.ToObjectReturnType0 toObject();

    public native DeleteItemRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
