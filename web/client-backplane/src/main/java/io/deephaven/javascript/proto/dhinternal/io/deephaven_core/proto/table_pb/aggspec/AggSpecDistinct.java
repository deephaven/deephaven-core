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
        name = "dhinternal.io.deephaven_core.proto.table_pb.AggSpec.AggSpecDistinct",
        namespace = JsPackage.GLOBAL)
public class AggSpecDistinct {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static AggSpecDistinct.ToObjectReturnType create() {
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
        static AggSpecDistinct.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        boolean isIncludeNulls();

        @JsProperty
        void setIncludeNulls(boolean includeNulls);
    }

    public static native AggSpecDistinct deserializeBinary(Uint8Array bytes);

    public static native AggSpecDistinct deserializeBinaryFromReader(
            AggSpecDistinct message, Object reader);

    public static native void serializeBinaryToWriter(AggSpecDistinct message, Object writer);

    public static native AggSpecDistinct.ToObjectReturnType toObject(
            boolean includeInstance, AggSpecDistinct msg);

    public native boolean getIncludeNulls();

    public native Uint8Array serializeBinary();

    public native void setIncludeNulls(boolean value);

    public native AggSpecDistinct.ToObjectReturnType0 toObject();

    public native AggSpecDistinct.ToObjectReturnType0 toObject(boolean includeInstance);
}
