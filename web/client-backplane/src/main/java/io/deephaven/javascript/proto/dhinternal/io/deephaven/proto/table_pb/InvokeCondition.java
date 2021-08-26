package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

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
        name = "dhinternal.io.deephaven.proto.table_pb.InvokeCondition",
        namespace = JsPackage.GLOBAL)
public class InvokeCondition {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TargetFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface LiteralFieldType {
                @JsOverlay
                static InvokeCondition.ToObjectReturnType.TargetFieldType.LiteralFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getDoubleValue();

                @JsProperty
                String getLongValue();

                @JsProperty
                String getNanoTimeValue();

                @JsProperty
                String getStringValue();

                @JsProperty
                boolean isBoolValue();

                @JsProperty
                void setBoolValue(boolean boolValue);

                @JsProperty
                void setDoubleValue(double doubleValue);

                @JsProperty
                void setLongValue(String longValue);

                @JsProperty
                void setNanoTimeValue(String nanoTimeValue);

                @JsProperty
                void setStringValue(String stringValue);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ReferenceFieldType {
                @JsOverlay
                static InvokeCondition.ToObjectReturnType.TargetFieldType.ReferenceFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumnName();

                @JsProperty
                void setColumnName(String columnName);
            }

            @JsOverlay
            static InvokeCondition.ToObjectReturnType.TargetFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            InvokeCondition.ToObjectReturnType.TargetFieldType.LiteralFieldType getLiteral();

            @JsProperty
            InvokeCondition.ToObjectReturnType.TargetFieldType.ReferenceFieldType getReference();

            @JsProperty
            void setLiteral(InvokeCondition.ToObjectReturnType.TargetFieldType.LiteralFieldType literal);

            @JsProperty
            void setReference(
                    InvokeCondition.ToObjectReturnType.TargetFieldType.ReferenceFieldType reference);
        }

        @JsOverlay
        static InvokeCondition.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<Object> getArgumentsList();

        @JsProperty
        String getMethod();

        @JsProperty
        InvokeCondition.ToObjectReturnType.TargetFieldType getTarget();

        @JsProperty
        void setArgumentsList(JsArray<Object> argumentsList);

        @JsOverlay
        default void setArgumentsList(Object[] argumentsList) {
            setArgumentsList(Js.<JsArray<Object>>uncheckedCast(argumentsList));
        }

        @JsProperty
        void setMethod(String method);

        @JsProperty
        void setTarget(InvokeCondition.ToObjectReturnType.TargetFieldType target);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TargetFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface LiteralFieldType {
                @JsOverlay
                static InvokeCondition.ToObjectReturnType0.TargetFieldType.LiteralFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getDoubleValue();

                @JsProperty
                String getLongValue();

                @JsProperty
                String getNanoTimeValue();

                @JsProperty
                String getStringValue();

                @JsProperty
                boolean isBoolValue();

                @JsProperty
                void setBoolValue(boolean boolValue);

                @JsProperty
                void setDoubleValue(double doubleValue);

                @JsProperty
                void setLongValue(String longValue);

                @JsProperty
                void setNanoTimeValue(String nanoTimeValue);

                @JsProperty
                void setStringValue(String stringValue);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ReferenceFieldType {
                @JsOverlay
                static InvokeCondition.ToObjectReturnType0.TargetFieldType.ReferenceFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumnName();

                @JsProperty
                void setColumnName(String columnName);
            }

            @JsOverlay
            static InvokeCondition.ToObjectReturnType0.TargetFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            InvokeCondition.ToObjectReturnType0.TargetFieldType.LiteralFieldType getLiteral();

            @JsProperty
            InvokeCondition.ToObjectReturnType0.TargetFieldType.ReferenceFieldType getReference();

            @JsProperty
            void setLiteral(InvokeCondition.ToObjectReturnType0.TargetFieldType.LiteralFieldType literal);

            @JsProperty
            void setReference(
                    InvokeCondition.ToObjectReturnType0.TargetFieldType.ReferenceFieldType reference);
        }

        @JsOverlay
        static InvokeCondition.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<Object> getArgumentsList();

        @JsProperty
        String getMethod();

        @JsProperty
        InvokeCondition.ToObjectReturnType0.TargetFieldType getTarget();

        @JsProperty
        void setArgumentsList(JsArray<Object> argumentsList);

        @JsOverlay
        default void setArgumentsList(Object[] argumentsList) {
            setArgumentsList(Js.<JsArray<Object>>uncheckedCast(argumentsList));
        }

        @JsProperty
        void setMethod(String method);

        @JsProperty
        void setTarget(InvokeCondition.ToObjectReturnType0.TargetFieldType target);
    }

    public static native InvokeCondition deserializeBinary(Uint8Array bytes);

    public static native InvokeCondition deserializeBinaryFromReader(
            InvokeCondition message, Object reader);

    public static native void serializeBinaryToWriter(InvokeCondition message, Object writer);

    public static native InvokeCondition.ToObjectReturnType toObject(
            boolean includeInstance, InvokeCondition msg);

    public native Value addArguments();

    public native Value addArguments(Value value, double index);

    public native Value addArguments(Value value);

    public native void clearArgumentsList();

    public native void clearTarget();

    public native JsArray<Value> getArgumentsList();

    public native String getMethod();

    public native Value getTarget();

    public native boolean hasTarget();

    public native Uint8Array serializeBinary();

    public native void setArgumentsList(JsArray<Value> value);

    @JsOverlay
    public final void setArgumentsList(Value[] value) {
        setArgumentsList(Js.<JsArray<Value>>uncheckedCast(value));
    }

    public native void setMethod(String value);

    public native void setTarget();

    public native void setTarget(Value value);

    public native InvokeCondition.ToObjectReturnType0 toObject();

    public native InvokeCondition.ToObjectReturnType0 toObject(boolean includeInstance);
}
