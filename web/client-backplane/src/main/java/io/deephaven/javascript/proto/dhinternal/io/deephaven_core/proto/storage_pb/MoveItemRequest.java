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
        name = "dhinternal.io.deephaven_core.proto.storage_pb.MoveItemRequest",
        namespace = JsPackage.GLOBAL)
public class MoveItemRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static MoveItemRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getNewPath();

        @JsProperty
        String getOldPath();

        @JsProperty
        boolean isAllowOverwrite();

        @JsProperty
        void setAllowOverwrite(boolean allowOverwrite);

        @JsProperty
        void setNewPath(String newPath);

        @JsProperty
        void setOldPath(String oldPath);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static MoveItemRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getNewPath();

        @JsProperty
        String getOldPath();

        @JsProperty
        boolean isAllowOverwrite();

        @JsProperty
        void setAllowOverwrite(boolean allowOverwrite);

        @JsProperty
        void setNewPath(String newPath);

        @JsProperty
        void setOldPath(String oldPath);
    }

    public static native MoveItemRequest deserializeBinary(Uint8Array bytes);

    public static native MoveItemRequest deserializeBinaryFromReader(
            MoveItemRequest message, Object reader);

    public static native void serializeBinaryToWriter(MoveItemRequest message, Object writer);

    public static native MoveItemRequest.ToObjectReturnType toObject(
            boolean includeInstance, MoveItemRequest msg);

    public native boolean getAllowOverwrite();

    public native String getNewPath();

    public native String getOldPath();

    public native Uint8Array serializeBinary();

    public native void setAllowOverwrite(boolean value);

    public native void setNewPath(String value);

    public native void setOldPath(String value);

    public native MoveItemRequest.ToObjectReturnType0 toObject();

    public native MoveItemRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
