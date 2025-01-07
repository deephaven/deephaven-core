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
        name = "dhinternal.io.deephaven_core.proto.table_pb.AggSpec.AggSpecPercentile",
        namespace = JsPackage.GLOBAL)
public class AggSpecPercentile {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static AggSpecPercentile.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getPercentile();

        @JsProperty
        boolean isAverageEvenlyDivided();

        @JsProperty
        void setAverageEvenlyDivided(boolean averageEvenlyDivided);

        @JsProperty
        void setPercentile(double percentile);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static AggSpecPercentile.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getPercentile();

        @JsProperty
        boolean isAverageEvenlyDivided();

        @JsProperty
        void setAverageEvenlyDivided(boolean averageEvenlyDivided);

        @JsProperty
        void setPercentile(double percentile);
    }

    public static native AggSpecPercentile deserializeBinary(Uint8Array bytes);

    public static native AggSpecPercentile deserializeBinaryFromReader(
            AggSpecPercentile message, Object reader);

    public static native void serializeBinaryToWriter(AggSpecPercentile message, Object writer);

    public static native AggSpecPercentile.ToObjectReturnType toObject(
            boolean includeInstance, AggSpecPercentile msg);

    public native boolean getAverageEvenlyDivided();

    public native double getPercentile();

    public native Uint8Array serializeBinary();

    public native void setAverageEvenlyDivided(boolean value);

    public native void setPercentile(double value);

    public native AggSpecPercentile.ToObjectReturnType0 toObject();

    public native AggSpecPercentile.ToObjectReturnType0 toObject(boolean includeInstance);
}
