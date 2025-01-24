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
        name = "dhinternal.io.deephaven_core.proto.table_pb.AggSpec.AggSpecSortedColumn",
        namespace = JsPackage.GLOBAL)
public class AggSpecSortedColumn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static AggSpecSortedColumn.ToObjectReturnType create() {
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
        static AggSpecSortedColumn.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnName();

        @JsProperty
        void setColumnName(String columnName);
    }

    public static native AggSpecSortedColumn deserializeBinary(Uint8Array bytes);

    public static native AggSpecSortedColumn deserializeBinaryFromReader(
            AggSpecSortedColumn message, Object reader);

    public static native void serializeBinaryToWriter(AggSpecSortedColumn message, Object writer);

    public static native AggSpecSortedColumn.ToObjectReturnType toObject(
            boolean includeInstance, AggSpecSortedColumn msg);

    public native String getColumnName();

    public native Uint8Array serializeBinary();

    public native void setColumnName(String value);

    public native AggSpecSortedColumn.ToObjectReturnType0 toObject();

    public native AggSpecSortedColumn.ToObjectReturnType0 toObject(boolean includeInstance);
}
