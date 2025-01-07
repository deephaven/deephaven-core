//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggregation;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.Aggregation.AggregationCount",
        namespace = JsPackage.GLOBAL)
public class AggregationCount {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static AggregationCount.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnName();

        @JsProperty
        void setColumnName(String columnName);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static AggregationCount.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnName();

        @JsProperty
        void setColumnName(String columnName);
    }

    public static native AggregationCount deserializeBinary(Uint8Array bytes);

    public static native AggregationCount deserializeBinaryFromReader(
            AggregationCount message, Object reader);

    public static native void serializeBinaryToWriter(AggregationCount message, Object writer);

    public static native AggregationCount.ToObjectReturnType toObject(
            boolean includeInstance, AggregationCount msg);

    public native String getColumnName();

    public native Uint8Array serializeBinary();

    public native void setColumnName(String value);

    public native AggregationCount.ToObjectReturnType0 toObject();

    public native AggregationCount.ToObjectReturnType0 toObject(boolean includeInstance);
}
