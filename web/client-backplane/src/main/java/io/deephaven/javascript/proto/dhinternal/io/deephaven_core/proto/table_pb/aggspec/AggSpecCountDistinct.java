//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.AggSpec.AggSpecCountDistinct",
        namespace = JsPackage.GLOBAL)
public class AggSpecCountDistinct {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static AggSpecCountDistinct.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        boolean isCountNulls();

        @JsProperty
        void setCountNulls(boolean countNulls);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static AggSpecCountDistinct.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        boolean isCountNulls();

        @JsProperty
        void setCountNulls(boolean countNulls);
    }

    public static native AggSpecCountDistinct deserializeBinary(Uint8Array bytes);

    public static native AggSpecCountDistinct deserializeBinaryFromReader(
            AggSpecCountDistinct message, Object reader);

    public static native void serializeBinaryToWriter(AggSpecCountDistinct message, Object writer);

    public static native AggSpecCountDistinct.ToObjectReturnType toObject(
            boolean includeInstance, AggSpecCountDistinct msg);

    public native boolean getCountNulls();

    public native Uint8Array serializeBinary();

    public native void setCountNulls(boolean value);

    public native AggSpecCountDistinct.ToObjectReturnType0 toObject();

    public native AggSpecCountDistinct.ToObjectReturnType0 toObject(boolean includeInstance);
}
