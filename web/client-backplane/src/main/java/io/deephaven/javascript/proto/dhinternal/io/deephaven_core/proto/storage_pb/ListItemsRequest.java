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
        name = "dhinternal.io.deephaven_core.proto.storage_pb.ListItemsRequest",
        namespace = JsPackage.GLOBAL)
public class ListItemsRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static ListItemsRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getFilterGlob();

        @JsProperty
        String getPath();

        @JsProperty
        void setFilterGlob(String filterGlob);

        @JsProperty
        void setPath(String path);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static ListItemsRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getFilterGlob();

        @JsProperty
        String getPath();

        @JsProperty
        void setFilterGlob(String filterGlob);

        @JsProperty
        void setPath(String path);
    }

    public static native ListItemsRequest deserializeBinary(Uint8Array bytes);

    public static native ListItemsRequest deserializeBinaryFromReader(
            ListItemsRequest message, Object reader);

    public static native void serializeBinaryToWriter(ListItemsRequest message, Object writer);

    public static native ListItemsRequest.ToObjectReturnType toObject(
            boolean includeInstance, ListItemsRequest msg);

    public native void clearFilterGlob();

    public native String getFilterGlob();

    public native String getPath();

    public native boolean hasFilterGlob();

    public native Uint8Array serializeBinary();

    public native void setFilterGlob(String value);

    public native void setPath(String value);

    public native ListItemsRequest.ToObjectReturnType0 toObject();

    public native ListItemsRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
