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
        name = "dhinternal.io.deephaven.proto.table_pb.UpdateByWindowScale.UpdateByWindowTicks",
        namespace = JsPackage.GLOBAL)
public class UpdateByWindowTicks {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static UpdateByWindowTicks.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getTicks();

        @JsProperty
        void setTicks(double ticks);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static UpdateByWindowTicks.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getTicks();

        @JsProperty
        void setTicks(double ticks);
    }

    public static native UpdateByWindowTicks deserializeBinary(Uint8Array bytes);

    public static native UpdateByWindowTicks deserializeBinaryFromReader(
            UpdateByWindowTicks message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByWindowTicks message, Object writer);

    public static native UpdateByWindowTicks.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByWindowTicks msg);

    public native double getTicks();

    public native Uint8Array serializeBinary();

    public native void setTicks(double value);

    public native UpdateByWindowTicks.ToObjectReturnType0 toObject();

    public native UpdateByWindowTicks.ToObjectReturnType0 toObject(boolean includeInstance);
}
