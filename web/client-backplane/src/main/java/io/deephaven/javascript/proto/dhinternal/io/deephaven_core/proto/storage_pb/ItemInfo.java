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
        name = "dhinternal.io.deephaven_core.proto.storage_pb.ItemInfo",
        namespace = JsPackage.GLOBAL)
public class ItemInfo {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static ItemInfo.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getEtag();

        @JsProperty
        String getPath();

        @JsProperty
        String getSize();

        @JsProperty
        double getType();

        @JsProperty
        void setEtag(String etag);

        @JsProperty
        void setPath(String path);

        @JsProperty
        void setSize(String size);

        @JsProperty
        void setType(double type);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static ItemInfo.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getEtag();

        @JsProperty
        String getPath();

        @JsProperty
        String getSize();

        @JsProperty
        double getType();

        @JsProperty
        void setEtag(String etag);

        @JsProperty
        void setPath(String path);

        @JsProperty
        void setSize(String size);

        @JsProperty
        void setType(double type);
    }

    public static native ItemInfo deserializeBinary(Uint8Array bytes);

    public static native ItemInfo deserializeBinaryFromReader(ItemInfo message, Object reader);

    public static native void serializeBinaryToWriter(ItemInfo message, Object writer);

    public static native ItemInfo.ToObjectReturnType toObject(boolean includeInstance, ItemInfo msg);

    public native void clearEtag();

    public native String getEtag();

    public native String getPath();

    public native String getSize();

    public native int getType();

    public native boolean hasEtag();

    public native Uint8Array serializeBinary();

    public native void setEtag(String value);

    public native void setPath(String value);

    public native void setSize(String value);

    public native void setType(int value);

    public native ItemInfo.ToObjectReturnType0 toObject();

    public native ItemInfo.ToObjectReturnType0 toObject(boolean includeInstance);
}
