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
        name = "dhinternal.io.deephaven_core.proto.table_pb.AggSpec.AggSpecTDigest",
        namespace = JsPackage.GLOBAL)
public class AggSpecTDigest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static AggSpecTDigest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getCompression();

        @JsProperty
        void setCompression(double compression);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static AggSpecTDigest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getCompression();

        @JsProperty
        void setCompression(double compression);
    }

    public static native AggSpecTDigest deserializeBinary(Uint8Array bytes);

    public static native AggSpecTDigest deserializeBinaryFromReader(
            AggSpecTDigest message, Object reader);

    public static native void serializeBinaryToWriter(AggSpecTDigest message, Object writer);

    public static native AggSpecTDigest.ToObjectReturnType toObject(
            boolean includeInstance, AggSpecTDigest msg);

    public native void clearCompression();

    public native double getCompression();

    public native boolean hasCompression();

    public native Uint8Array serializeBinary();

    public native void setCompression(double value);

    public native AggSpecTDigest.ToObjectReturnType0 toObject();

    public native AggSpecTDigest.ToObjectReturnType0 toObject(boolean includeInstance);
}
