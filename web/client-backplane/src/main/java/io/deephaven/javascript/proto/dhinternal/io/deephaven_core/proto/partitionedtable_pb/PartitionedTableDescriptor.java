//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.partitionedtable_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.partitionedtable_pb.PartitionedTableDescriptor",
        namespace = JsPackage.GLOBAL)
public class PartitionedTableDescriptor {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetConstituentDefinitionSchemaUnionType {
        @JsOverlay
        static PartitionedTableDescriptor.GetConstituentDefinitionSchemaUnionType of(Object o) {
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
    public interface SetConstituentDefinitionSchemaValueUnionType {
        @JsOverlay
        static PartitionedTableDescriptor.SetConstituentDefinitionSchemaValueUnionType of(Object o) {
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
        public interface GetConstituentDefinitionSchemaUnionType {
            @JsOverlay
            static PartitionedTableDescriptor.ToObjectReturnType.GetConstituentDefinitionSchemaUnionType of(Object o) {
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
        static PartitionedTableDescriptor.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getConstituentColumnName();

        @JsProperty
        PartitionedTableDescriptor.ToObjectReturnType.GetConstituentDefinitionSchemaUnionType getConstituentDefinitionSchema();

        @JsProperty
        JsArray<String> getKeyColumnNamesList();

        @JsProperty
        boolean isConstituentChangesPermitted();

        @JsProperty
        boolean isUniqueKeys();

        @JsProperty
        void setConstituentChangesPermitted(boolean constituentChangesPermitted);

        @JsProperty
        void setConstituentColumnName(String constituentColumnName);

        @JsProperty
        void setConstituentDefinitionSchema(
                PartitionedTableDescriptor.ToObjectReturnType.GetConstituentDefinitionSchemaUnionType constituentDefinitionSchema);

        @JsOverlay
        default void setConstituentDefinitionSchema(String constituentDefinitionSchema) {
            setConstituentDefinitionSchema(
                    Js.<PartitionedTableDescriptor.ToObjectReturnType.GetConstituentDefinitionSchemaUnionType>uncheckedCast(
                            constituentDefinitionSchema));
        }

        @JsOverlay
        default void setConstituentDefinitionSchema(Uint8Array constituentDefinitionSchema) {
            setConstituentDefinitionSchema(
                    Js.<PartitionedTableDescriptor.ToObjectReturnType.GetConstituentDefinitionSchemaUnionType>uncheckedCast(
                            constituentDefinitionSchema));
        }

        @JsProperty
        void setKeyColumnNamesList(JsArray<String> keyColumnNamesList);

        @JsOverlay
        default void setKeyColumnNamesList(String[] keyColumnNamesList) {
            setKeyColumnNamesList(Js.<JsArray<String>>uncheckedCast(keyColumnNamesList));
        }

        @JsProperty
        void setUniqueKeys(boolean uniqueKeys);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetConstituentDefinitionSchemaUnionType {
            @JsOverlay
            static PartitionedTableDescriptor.ToObjectReturnType0.GetConstituentDefinitionSchemaUnionType of(Object o) {
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
        static PartitionedTableDescriptor.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getConstituentColumnName();

        @JsProperty
        PartitionedTableDescriptor.ToObjectReturnType0.GetConstituentDefinitionSchemaUnionType getConstituentDefinitionSchema();

        @JsProperty
        JsArray<String> getKeyColumnNamesList();

        @JsProperty
        boolean isConstituentChangesPermitted();

        @JsProperty
        boolean isUniqueKeys();

        @JsProperty
        void setConstituentChangesPermitted(boolean constituentChangesPermitted);

        @JsProperty
        void setConstituentColumnName(String constituentColumnName);

        @JsProperty
        void setConstituentDefinitionSchema(
                PartitionedTableDescriptor.ToObjectReturnType0.GetConstituentDefinitionSchemaUnionType constituentDefinitionSchema);

        @JsOverlay
        default void setConstituentDefinitionSchema(String constituentDefinitionSchema) {
            setConstituentDefinitionSchema(
                    Js.<PartitionedTableDescriptor.ToObjectReturnType0.GetConstituentDefinitionSchemaUnionType>uncheckedCast(
                            constituentDefinitionSchema));
        }

        @JsOverlay
        default void setConstituentDefinitionSchema(Uint8Array constituentDefinitionSchema) {
            setConstituentDefinitionSchema(
                    Js.<PartitionedTableDescriptor.ToObjectReturnType0.GetConstituentDefinitionSchemaUnionType>uncheckedCast(
                            constituentDefinitionSchema));
        }

        @JsProperty
        void setKeyColumnNamesList(JsArray<String> keyColumnNamesList);

        @JsOverlay
        default void setKeyColumnNamesList(String[] keyColumnNamesList) {
            setKeyColumnNamesList(Js.<JsArray<String>>uncheckedCast(keyColumnNamesList));
        }

        @JsProperty
        void setUniqueKeys(boolean uniqueKeys);
    }

    public static native PartitionedTableDescriptor deserializeBinary(Uint8Array bytes);

    public static native PartitionedTableDescriptor deserializeBinaryFromReader(
            PartitionedTableDescriptor message, Object reader);

    public static native void serializeBinaryToWriter(
            PartitionedTableDescriptor message, Object writer);

    public static native PartitionedTableDescriptor.ToObjectReturnType toObject(
            boolean includeInstance, PartitionedTableDescriptor msg);

    public native String addKeyColumnNames(String value, double index);

    public native String addKeyColumnNames(String value);

    public native void clearKeyColumnNamesList();

    public native boolean getConstituentChangesPermitted();

    public native String getConstituentColumnName();

    public native PartitionedTableDescriptor.GetConstituentDefinitionSchemaUnionType getConstituentDefinitionSchema();

    public native String getConstituentDefinitionSchema_asB64();

    public native Uint8Array getConstituentDefinitionSchema_asU8();

    public native JsArray<String> getKeyColumnNamesList();

    public native boolean getUniqueKeys();

    public native Uint8Array serializeBinary();

    public native void setConstituentChangesPermitted(boolean value);

    public native void setConstituentColumnName(String value);

    public native void setConstituentDefinitionSchema(
            PartitionedTableDescriptor.SetConstituentDefinitionSchemaValueUnionType value);

    @JsOverlay
    public final void setConstituentDefinitionSchema(String value) {
        setConstituentDefinitionSchema(
                Js.<PartitionedTableDescriptor.SetConstituentDefinitionSchemaValueUnionType>uncheckedCast(
                        value));
    }

    @JsOverlay
    public final void setConstituentDefinitionSchema(Uint8Array value) {
        setConstituentDefinitionSchema(
                Js.<PartitionedTableDescriptor.SetConstituentDefinitionSchemaValueUnionType>uncheckedCast(
                        value));
    }

    public native void setKeyColumnNamesList(JsArray<String> value);

    @JsOverlay
    public final void setKeyColumnNamesList(String[] value) {
        setKeyColumnNamesList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setUniqueKeys(boolean value);

    public native PartitionedTableDescriptor.ToObjectReturnType0 toObject();

    public native PartitionedTableDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}
