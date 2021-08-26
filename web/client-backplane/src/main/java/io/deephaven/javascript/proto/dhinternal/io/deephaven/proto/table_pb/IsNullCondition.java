package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.IsNullCondition",
        namespace = JsPackage.GLOBAL)
public class IsNullCondition {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ReferenceFieldType {
            @JsOverlay
            static IsNullCondition.ToObjectReturnType.ReferenceFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getColumnName();

            @JsProperty
            void setColumnName(String columnName);
        }

        @JsOverlay
        static IsNullCondition.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        IsNullCondition.ToObjectReturnType.ReferenceFieldType getReference();

        @JsProperty
        void setReference(IsNullCondition.ToObjectReturnType.ReferenceFieldType reference);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ReferenceFieldType {
            @JsOverlay
            static IsNullCondition.ToObjectReturnType0.ReferenceFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getColumnName();

            @JsProperty
            void setColumnName(String columnName);
        }

        @JsOverlay
        static IsNullCondition.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        IsNullCondition.ToObjectReturnType0.ReferenceFieldType getReference();

        @JsProperty
        void setReference(IsNullCondition.ToObjectReturnType0.ReferenceFieldType reference);
    }

    public static native IsNullCondition deserializeBinary(Uint8Array bytes);

    public static native IsNullCondition deserializeBinaryFromReader(
            IsNullCondition message, Object reader);

    public static native void serializeBinaryToWriter(IsNullCondition message, Object writer);

    public static native IsNullCondition.ToObjectReturnType toObject(
            boolean includeInstance, IsNullCondition msg);

    public native void clearReference();

    public native Reference getReference();

    public native boolean hasReference();

    public native Uint8Array serializeBinary();

    public native void setReference();

    public native void setReference(Reference value);

    public native IsNullCondition.ToObjectReturnType0 toObject();

    public native IsNullCondition.ToObjectReturnType0 toObject(boolean includeInstance);
}
