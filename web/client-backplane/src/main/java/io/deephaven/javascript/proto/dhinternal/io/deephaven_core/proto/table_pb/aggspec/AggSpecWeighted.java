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
        name = "dhinternal.io.deephaven_core.proto.table_pb.AggSpec.AggSpecWeighted",
        namespace = JsPackage.GLOBAL)
public class AggSpecWeighted {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static AggSpecWeighted.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getWeightColumn();

        @JsProperty
        void setWeightColumn(String weightColumn);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static AggSpecWeighted.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getWeightColumn();

        @JsProperty
        void setWeightColumn(String weightColumn);
    }

    public static native AggSpecWeighted deserializeBinary(Uint8Array bytes);

    public static native AggSpecWeighted deserializeBinaryFromReader(
            AggSpecWeighted message, Object reader);

    public static native void serializeBinaryToWriter(AggSpecWeighted message, Object writer);

    public static native AggSpecWeighted.ToObjectReturnType toObject(
            boolean includeInstance, AggSpecWeighted msg);

    public native String getWeightColumn();

    public native Uint8Array serializeBinary();

    public native void setWeightColumn(String value);

    public native AggSpecWeighted.ToObjectReturnType0 toObject();

    public native AggSpecWeighted.ToObjectReturnType0 toObject(boolean includeInstance);
}
