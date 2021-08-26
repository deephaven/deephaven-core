package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.sortdescriptor.SortDirectionMap;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.SortDescriptor",
        namespace = JsPackage.GLOBAL)
public class SortDescriptor {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static SortDescriptor.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnName();

        @JsProperty
        double getDirection();

        @JsProperty
        boolean isIsAbsolute();

        @JsProperty
        void setColumnName(String columnName);

        @JsProperty
        void setDirection(double direction);

        @JsProperty
        void setIsAbsolute(boolean isAbsolute);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static SortDescriptor.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnName();

        @JsProperty
        double getDirection();

        @JsProperty
        boolean isIsAbsolute();

        @JsProperty
        void setColumnName(String columnName);

        @JsProperty
        void setDirection(double direction);

        @JsProperty
        void setIsAbsolute(boolean isAbsolute);
    }

    public static SortDirectionMap SortDirection;

    public static native SortDescriptor deserializeBinary(Uint8Array bytes);

    public static native SortDescriptor deserializeBinaryFromReader(
            SortDescriptor message, Object reader);

    public static native void serializeBinaryToWriter(SortDescriptor message, Object writer);

    public static native SortDescriptor.ToObjectReturnType toObject(
            boolean includeInstance, SortDescriptor msg);

    public native String getColumnName();

    public native double getDirection();

    public native boolean getIsAbsolute();

    public native Uint8Array serializeBinary();

    public native void setColumnName(String value);

    public native void setDirection(double value);

    public native void setIsAbsolute(boolean value);

    public native SortDescriptor.ToObjectReturnType0 toObject();

    public native SortDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}
