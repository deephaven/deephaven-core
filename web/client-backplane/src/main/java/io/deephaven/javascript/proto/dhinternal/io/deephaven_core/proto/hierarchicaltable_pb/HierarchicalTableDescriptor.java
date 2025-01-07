//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.HierarchicalTableDescriptor",
        namespace = JsPackage.GLOBAL)
public class HierarchicalTableDescriptor {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetSnapshotSchemaUnionType {
        @JsOverlay
        static HierarchicalTableDescriptor.GetSnapshotSchemaUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
            return (Object) this instanceof Uint8Array;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SetSnapshotSchemaValueUnionType {
        @JsOverlay
        static HierarchicalTableDescriptor.SetSnapshotSchemaValueUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
            return (Object) this instanceof Uint8Array;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetSnapshotSchemaUnionType {
            @JsOverlay
            static HierarchicalTableDescriptor.ToObjectReturnType.GetSnapshotSchemaUnionType of(
                    Object o) {
                return Js.cast(o);
            }

            @JsOverlay
            default String asString() {
                return Js.asString(this);
            }

            @JsOverlay
            default Uint8Array asUint8Array() {
                return Js.cast(this);
            }

            @JsOverlay
            default boolean isString() {
                return (Object) this instanceof String;
            }

            @JsOverlay
            default boolean isUint8Array() {
                return (Object) this instanceof Uint8Array;
            }
        }

        @JsOverlay
        static HierarchicalTableDescriptor.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        HierarchicalTableDescriptor.ToObjectReturnType.GetSnapshotSchemaUnionType getSnapshotSchema();

        @JsProperty
        boolean isIsStatic();

        @JsProperty
        void setIsStatic(boolean isStatic);

        @JsProperty
        void setSnapshotSchema(
                HierarchicalTableDescriptor.ToObjectReturnType.GetSnapshotSchemaUnionType snapshotSchema);

        @JsOverlay
        default void setSnapshotSchema(String snapshotSchema) {
            setSnapshotSchema(
                    Js.<HierarchicalTableDescriptor.ToObjectReturnType.GetSnapshotSchemaUnionType>uncheckedCast(
                            snapshotSchema));
        }

        @JsOverlay
        default void setSnapshotSchema(Uint8Array snapshotSchema) {
            setSnapshotSchema(
                    Js.<HierarchicalTableDescriptor.ToObjectReturnType.GetSnapshotSchemaUnionType>uncheckedCast(
                            snapshotSchema));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetSnapshotSchemaUnionType {
            @JsOverlay
            static HierarchicalTableDescriptor.ToObjectReturnType0.GetSnapshotSchemaUnionType of(
                    Object o) {
                return Js.cast(o);
            }

            @JsOverlay
            default String asString() {
                return Js.asString(this);
            }

            @JsOverlay
            default Uint8Array asUint8Array() {
                return Js.cast(this);
            }

            @JsOverlay
            default boolean isString() {
                return (Object) this instanceof String;
            }

            @JsOverlay
            default boolean isUint8Array() {
                return (Object) this instanceof Uint8Array;
            }
        }

        @JsOverlay
        static HierarchicalTableDescriptor.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        HierarchicalTableDescriptor.ToObjectReturnType0.GetSnapshotSchemaUnionType getSnapshotSchema();

        @JsProperty
        boolean isIsStatic();

        @JsProperty
        void setIsStatic(boolean isStatic);

        @JsProperty
        void setSnapshotSchema(
                HierarchicalTableDescriptor.ToObjectReturnType0.GetSnapshotSchemaUnionType snapshotSchema);

        @JsOverlay
        default void setSnapshotSchema(String snapshotSchema) {
            setSnapshotSchema(
                    Js.<HierarchicalTableDescriptor.ToObjectReturnType0.GetSnapshotSchemaUnionType>uncheckedCast(
                            snapshotSchema));
        }

        @JsOverlay
        default void setSnapshotSchema(Uint8Array snapshotSchema) {
            setSnapshotSchema(
                    Js.<HierarchicalTableDescriptor.ToObjectReturnType0.GetSnapshotSchemaUnionType>uncheckedCast(
                            snapshotSchema));
        }
    }

    public static native HierarchicalTableDescriptor deserializeBinary(Uint8Array bytes);

    public static native HierarchicalTableDescriptor deserializeBinaryFromReader(
            HierarchicalTableDescriptor message, Object reader);

    public static native void serializeBinaryToWriter(
            HierarchicalTableDescriptor message, Object writer);

    public static native HierarchicalTableDescriptor.ToObjectReturnType toObject(
            boolean includeInstance, HierarchicalTableDescriptor msg);

    public native boolean getIsStatic();

    public native HierarchicalTableDescriptor.GetSnapshotSchemaUnionType getSnapshotSchema();

    public native String getSnapshotSchema_asB64();

    public native Uint8Array getSnapshotSchema_asU8();

    public native Uint8Array serializeBinary();

    public native void setIsStatic(boolean value);

    public native void setSnapshotSchema(
            HierarchicalTableDescriptor.SetSnapshotSchemaValueUnionType value);

    @JsOverlay
    public final void setSnapshotSchema(String value) {
        setSnapshotSchema(
                Js.<HierarchicalTableDescriptor.SetSnapshotSchemaValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setSnapshotSchema(Uint8Array value) {
        setSnapshotSchema(
                Js.<HierarchicalTableDescriptor.SetSnapshotSchemaValueUnionType>uncheckedCast(value));
    }

    public native HierarchicalTableDescriptor.ToObjectReturnType0 toObject();

    public native HierarchicalTableDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}
