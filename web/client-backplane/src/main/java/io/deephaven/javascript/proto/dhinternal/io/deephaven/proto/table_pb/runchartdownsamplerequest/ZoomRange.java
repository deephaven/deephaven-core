package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.runchartdownsamplerequest;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.RunChartDownsampleRequest.ZoomRange",
    namespace = JsPackage.GLOBAL)
public class ZoomRange {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static ZoomRange.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getMaxDateNanos();

        @JsProperty
        String getMinDateNanos();

        @JsProperty
        void setMaxDateNanos(String maxDateNanos);

        @JsProperty
        void setMinDateNanos(String minDateNanos);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static ZoomRange.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getMaxDateNanos();

        @JsProperty
        String getMinDateNanos();

        @JsProperty
        void setMaxDateNanos(String maxDateNanos);

        @JsProperty
        void setMinDateNanos(String minDateNanos);
    }

    public static native ZoomRange deserializeBinary(Uint8Array bytes);

    public static native ZoomRange deserializeBinaryFromReader(ZoomRange message, Object reader);

    public static native void serializeBinaryToWriter(ZoomRange message, Object writer);

    public static native ZoomRange.ToObjectReturnType toObject(
        boolean includeInstance, ZoomRange msg);

    public native void clearMaxDateNanos();

    public native void clearMinDateNanos();

    public native String getMaxDateNanos();

    public native String getMinDateNanos();

    public native boolean hasMaxDateNanos();

    public native boolean hasMinDateNanos();

    public native Uint8Array serializeBinary();

    public native void setMaxDateNanos(String value);

    public native void setMinDateNanos(String value);

    public native ZoomRange.ToObjectReturnType0 toObject();

    public native ZoomRange.ToObjectReturnType0 toObject(boolean includeInstance);
}
