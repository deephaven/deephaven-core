package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.MultiSeriesSourceDescriptor",
    namespace = JsPackage.GLOBAL)
public class MultiSeriesSourceDescriptor {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static MultiSeriesSourceDescriptor.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getAxisId();

        @JsProperty
        String getColumnName();

        @JsProperty
        double getTableMapId();

        @JsProperty
        double getType();

        @JsProperty
        void setAxisId(String axisId);

        @JsProperty
        void setColumnName(String columnName);

        @JsProperty
        void setTableMapId(double tableMapId);

        @JsProperty
        void setType(double type);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static MultiSeriesSourceDescriptor.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getAxisId();

        @JsProperty
        String getColumnName();

        @JsProperty
        double getTableMapId();

        @JsProperty
        double getType();

        @JsProperty
        void setAxisId(String axisId);

        @JsProperty
        void setColumnName(String columnName);

        @JsProperty
        void setTableMapId(double tableMapId);

        @JsProperty
        void setType(double type);
    }

    public static native MultiSeriesSourceDescriptor deserializeBinary(Uint8Array bytes);

    public static native MultiSeriesSourceDescriptor deserializeBinaryFromReader(
        MultiSeriesSourceDescriptor message, Object reader);

    public static native void serializeBinaryToWriter(
        MultiSeriesSourceDescriptor message, Object writer);

    public static native MultiSeriesSourceDescriptor.ToObjectReturnType toObject(
        boolean includeInstance, MultiSeriesSourceDescriptor msg);

    public native String getAxisId();

    public native String getColumnName();

    public native int getTableMapId();

    public native int getType();

    public native Uint8Array serializeBinary();

    public native void setAxisId(String value);

    public native void setColumnName(String value);

    public native void setTableMapId(int value);

    public native void setType(int value);

    public native MultiSeriesSourceDescriptor.ToObjectReturnType0 toObject();

    public native MultiSeriesSourceDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}
