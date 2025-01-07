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
        name = "dhinternal.io.deephaven_core.proto.table_pb.AggSpec.AggSpecApproximatePercentile",
        namespace = JsPackage.GLOBAL)
public class AggSpecApproximatePercentile {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static AggSpecApproximatePercentile.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getCompression();

        @JsProperty
        double getPercentile();

        @JsProperty
        void setCompression(double compression);

        @JsProperty
        void setPercentile(double percentile);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static AggSpecApproximatePercentile.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getCompression();

        @JsProperty
        double getPercentile();

        @JsProperty
        void setCompression(double compression);

        @JsProperty
        void setPercentile(double percentile);
    }

    public static native AggSpecApproximatePercentile deserializeBinary(Uint8Array bytes);

    public static native AggSpecApproximatePercentile deserializeBinaryFromReader(
            AggSpecApproximatePercentile message, Object reader);

    public static native void serializeBinaryToWriter(
            AggSpecApproximatePercentile message, Object writer);

    public static native AggSpecApproximatePercentile.ToObjectReturnType toObject(
            boolean includeInstance, AggSpecApproximatePercentile msg);

    public native void clearCompression();

    public native double getCompression();

    public native double getPercentile();

    public native boolean hasCompression();

    public native Uint8Array serializeBinary();

    public native void setCompression(double value);

    public native void setPercentile(double value);

    public native AggSpecApproximatePercentile.ToObjectReturnType0 toObject();

    public native AggSpecApproximatePercentile.ToObjectReturnType0 toObject(boolean includeInstance);
}
