package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebywindowscale;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.UpdateByWindowScale.UpdateByWindowTime",
        namespace = JsPackage.GLOBAL)
public class UpdateByWindowTime {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static UpdateByWindowTime.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumn();

        @JsProperty
        String getDurationString();

        @JsProperty
        String getNanos();

        @JsProperty
        void setColumn(String column);

        @JsProperty
        void setDurationString(String durationString);

        @JsProperty
        void setNanos(String nanos);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static UpdateByWindowTime.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumn();

        @JsProperty
        String getDurationString();

        @JsProperty
        String getNanos();

        @JsProperty
        void setColumn(String column);

        @JsProperty
        void setDurationString(String durationString);

        @JsProperty
        void setNanos(String nanos);
    }

    public static native UpdateByWindowTime deserializeBinary(Uint8Array bytes);

    public static native UpdateByWindowTime deserializeBinaryFromReader(
            UpdateByWindowTime message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByWindowTime message, Object writer);

    public static native UpdateByWindowTime.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByWindowTime msg);

    public native void clearDurationString();

    public native void clearNanos();

    public native String getColumn();

    public native String getDurationString();

    public native String getNanos();

    public native int getWindowCase();

    public native boolean hasDurationString();

    public native boolean hasNanos();

    public native Uint8Array serializeBinary();

    public native void setColumn(String value);

    public native void setDurationString(String value);

    public native void setNanos(String value);

    public native UpdateByWindowTime.ToObjectReturnType0 toObject();

    public native UpdateByWindowTime.ToObjectReturnType0 toObject(boolean includeInstance);
}
