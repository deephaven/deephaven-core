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
        name = "dhinternal.io.deephaven_core.proto.table_pb.Aggregation.AggregationPartition",
        namespace = JsPackage.GLOBAL)
public class AggregationPartition {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static AggregationPartition.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnName();

        @JsProperty
        boolean isIncludeGroupByColumns();

        @JsProperty
        void setColumnName(String columnName);

        @JsProperty
        void setIncludeGroupByColumns(boolean includeGroupByColumns);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static AggregationPartition.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnName();

        @JsProperty
        boolean isIncludeGroupByColumns();

        @JsProperty
        void setColumnName(String columnName);

        @JsProperty
        void setIncludeGroupByColumns(boolean includeGroupByColumns);
    }

    public static native AggregationPartition deserializeBinary(Uint8Array bytes);

    public static native AggregationPartition deserializeBinaryFromReader(
            AggregationPartition message, Object reader);

    public static native void serializeBinaryToWriter(AggregationPartition message, Object writer);

    public static native AggregationPartition.ToObjectReturnType toObject(
            boolean includeInstance, AggregationPartition msg);

    public native String getColumnName();

    public native boolean getIncludeGroupByColumns();

    public native Uint8Array serializeBinary();

    public native void setColumnName(String value);

    public native void setIncludeGroupByColumns(boolean value);

    public native AggregationPartition.ToObjectReturnType0 toObject();

    public native AggregationPartition.ToObjectReturnType0 toObject(boolean includeInstance);
}
