package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.AggSpec.AggSpecUnique",
        namespace = JsPackage.GLOBAL)
public class AggSpecUnique {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static AggSpecUnique.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        boolean isIncludeNulls();

        @JsProperty
        void setIncludeNulls(boolean includeNulls);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static AggSpecUnique.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        boolean isIncludeNulls();

        @JsProperty
        void setIncludeNulls(boolean includeNulls);
    }

    public static native AggSpecUnique deserializeBinary(Uint8Array bytes);

    public static native AggSpecUnique deserializeBinaryFromReader(
            AggSpecUnique message, Object reader);

    public static native void serializeBinaryToWriter(AggSpecUnique message, Object writer);

    public static native AggSpecUnique.ToObjectReturnType toObject(
            boolean includeInstance, AggSpecUnique msg);

    public native void clearIncludeNulls();

    public native boolean getIncludeNulls();

    public native boolean hasIncludeNulls();

    public native Uint8Array serializeBinary();

    public native void setIncludeNulls(boolean value);

    public native AggSpecUnique.ToObjectReturnType0 toObject();

    public native AggSpecUnique.ToObjectReturnType0 toObject(boolean includeInstance);
}
