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
        name = "dhinternal.io.deephaven_core.proto.table_pb.AggSpec.AggSpecMedian",
        namespace = JsPackage.GLOBAL)
public class AggSpecMedian {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static AggSpecMedian.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        boolean isAverageEvenlyDivided();

        @JsProperty
        void setAverageEvenlyDivided(boolean averageEvenlyDivided);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static AggSpecMedian.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        boolean isAverageEvenlyDivided();

        @JsProperty
        void setAverageEvenlyDivided(boolean averageEvenlyDivided);
    }

    public static native AggSpecMedian deserializeBinary(Uint8Array bytes);

    public static native AggSpecMedian deserializeBinaryFromReader(
            AggSpecMedian message, Object reader);

    public static native void serializeBinaryToWriter(AggSpecMedian message, Object writer);

    public static native AggSpecMedian.ToObjectReturnType toObject(
            boolean includeInstance, AggSpecMedian msg);

    public native boolean getAverageEvenlyDivided();

    public native Uint8Array serializeBinary();

    public native void setAverageEvenlyDivided(boolean value);

    public native AggSpecMedian.ToObjectReturnType0 toObject();

    public native AggSpecMedian.ToObjectReturnType0 toObject(boolean includeInstance);
}
