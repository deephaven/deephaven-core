//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.Selectable;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.UpdateViewRequest",
        namespace = JsPackage.GLOBAL)
public class UpdateViewRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ColumnSpecFieldType {
            @JsOverlay
            static UpdateViewRequest.ToObjectReturnType.ColumnSpecFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getRaw();

            @JsProperty
            void setRaw(String raw);
        }

        @JsOverlay
        static UpdateViewRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateViewRequest.ToObjectReturnType.ColumnSpecFieldType getColumnSpec();

        @JsProperty
        double getNodeType();

        @JsProperty
        void setColumnSpec(UpdateViewRequest.ToObjectReturnType.ColumnSpecFieldType columnSpec);

        @JsProperty
        void setNodeType(double nodeType);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ColumnSpecFieldType {
            @JsOverlay
            static UpdateViewRequest.ToObjectReturnType0.ColumnSpecFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getRaw();

            @JsProperty
            void setRaw(String raw);
        }

        @JsOverlay
        static UpdateViewRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateViewRequest.ToObjectReturnType0.ColumnSpecFieldType getColumnSpec();

        @JsProperty
        double getNodeType();

        @JsProperty
        void setColumnSpec(UpdateViewRequest.ToObjectReturnType0.ColumnSpecFieldType columnSpec);

        @JsProperty
        void setNodeType(double nodeType);
    }

    public static native UpdateViewRequest deserializeBinary(Uint8Array bytes);

    public static native UpdateViewRequest deserializeBinaryFromReader(
            UpdateViewRequest message, Object reader);

    public static native void serializeBinaryToWriter(UpdateViewRequest message, Object writer);

    public static native UpdateViewRequest.ToObjectReturnType toObject(
            boolean includeInstance, UpdateViewRequest msg);

    public native void clearColumnSpec();

    public native Selectable getColumnSpec();

    public native int getNodeType();

    public native boolean hasColumnSpec();

    public native Uint8Array serializeBinary();

    public native void setColumnSpec();

    public native void setColumnSpec(Selectable value);

    public native void setNodeType(int value);

    public native UpdateViewRequest.ToObjectReturnType0 toObject();

    public native UpdateViewRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
