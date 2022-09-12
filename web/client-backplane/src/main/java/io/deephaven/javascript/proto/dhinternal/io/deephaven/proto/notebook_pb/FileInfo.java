package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.notebook_pb.FileInfo",
        namespace = JsPackage.GLOBAL)
public class FileInfo {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static FileInfo.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getKind();

        @JsProperty
        String getPath();

        @JsProperty
        String getSize();

        @JsProperty
        void setKind(double kind);

        @JsProperty
        void setPath(String path);

        @JsProperty
        void setSize(String size);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static FileInfo.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getKind();

        @JsProperty
        String getPath();

        @JsProperty
        String getSize();

        @JsProperty
        void setKind(double kind);

        @JsProperty
        void setPath(String path);

        @JsProperty
        void setSize(String size);
    }

    public static native FileInfo deserializeBinary(Uint8Array bytes);

    public static native FileInfo deserializeBinaryFromReader(FileInfo message, Object reader);

    public static native void serializeBinaryToWriter(FileInfo message, Object writer);

    public static native FileInfo.ToObjectReturnType toObject(boolean includeInstance, FileInfo msg);

    public native int getKind();

    public native String getPath();

    public native String getSize();

    public native Uint8Array serializeBinary();

    public native void setKind(int value);

    public native void setPath(String value);

    public native void setSize(String value);

    public native FileInfo.ToObjectReturnType0 toObject();

    public native FileInfo.ToObjectReturnType0 toObject(boolean includeInstance);
}
