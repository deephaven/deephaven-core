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
        name = "dhinternal.io.deephaven.proto.table_pb.UpdateByDeltaOptions",
        namespace = JsPackage.GLOBAL)
public class UpdateByDeltaOptions {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static UpdateByDeltaOptions.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getNullBehavior();

        @JsProperty
        void setNullBehavior(double nullBehavior);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static UpdateByDeltaOptions.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getNullBehavior();

        @JsProperty
        void setNullBehavior(double nullBehavior);
    }

    public static native UpdateByDeltaOptions deserializeBinary(Uint8Array bytes);

    public static native UpdateByDeltaOptions deserializeBinaryFromReader(
            UpdateByDeltaOptions message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByDeltaOptions message, Object writer);

    public static native UpdateByDeltaOptions.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByDeltaOptions msg);

    public native double getNullBehavior();

    public native Uint8Array serializeBinary();

    public native void setNullBehavior(double value);

    public native UpdateByDeltaOptions.ToObjectReturnType0 toObject();

    public native UpdateByDeltaOptions.ToObjectReturnType0 toObject(boolean includeInstance);
}
