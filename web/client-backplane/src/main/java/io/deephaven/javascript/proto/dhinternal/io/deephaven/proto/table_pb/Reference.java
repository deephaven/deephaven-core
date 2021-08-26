package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.Reference",
    namespace = JsPackage.GLOBAL)
public class Reference {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static Reference.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnName();

        @JsProperty
        void setColumnName(String columnName);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static Reference.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnName();

        @JsProperty
        void setColumnName(String columnName);
    }

    public static native Reference deserializeBinary(Uint8Array bytes);

    public static native Reference deserializeBinaryFromReader(Reference message, Object reader);

    public static native void serializeBinaryToWriter(Reference message, Object writer);

    public static native Reference.ToObjectReturnType toObject(
        boolean includeInstance, Reference msg);

    public native String getColumnName();

    public native Uint8Array serializeBinary();

    public native void setColumnName(String value);

    public native Reference.ToObjectReturnType0 toObject();

    public native Reference.ToObjectReturnType0 toObject(boolean includeInstance);
}
