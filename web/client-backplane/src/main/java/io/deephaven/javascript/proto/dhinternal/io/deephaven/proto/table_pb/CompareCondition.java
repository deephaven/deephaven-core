package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.comparecondition.CompareOperationMap;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.CompareCondition",
        namespace = JsPackage.GLOBAL)
public class CompareCondition {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface LhsFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface LiteralFieldType {
                @JsOverlay
                static CompareCondition.ToObjectReturnType.LhsFieldType.LiteralFieldType create() {
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
                static CompareCondition.ToObjectReturnType.LhsFieldType.ReferenceFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumnName();

                @JsProperty
                void setColumnName(String columnName);
            }

            @JsOverlay
            static CompareCondition.ToObjectReturnType.LhsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            CompareCondition.ToObjectReturnType.LhsFieldType.LiteralFieldType getLiteral();

            @JsProperty
            CompareCondition.ToObjectReturnType.LhsFieldType.ReferenceFieldType getReference();

            @JsProperty
            void setLiteral(CompareCondition.ToObjectReturnType.LhsFieldType.LiteralFieldType literal);

            @JsProperty
            void setReference(
                    CompareCondition.ToObjectReturnType.LhsFieldType.ReferenceFieldType reference);
        }

        @JsOverlay
        static CompareCondition.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getCaseSensitivity();

        @JsProperty
        CompareCondition.ToObjectReturnType.LhsFieldType getLhs();

        @JsProperty
        double getOperation();

        @JsProperty
        Object getRhs();

        @JsProperty
        void setCaseSensitivity(double caseSensitivity);

        @JsProperty
        void setLhs(CompareCondition.ToObjectReturnType.LhsFieldType lhs);

        @JsProperty
        void setOperation(double operation);

        @JsProperty
        void setRhs(Object rhs);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface LhsFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface LiteralFieldType {
                @JsOverlay
                static CompareCondition.ToObjectReturnType0.LhsFieldType.LiteralFieldType create() {
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
                static CompareCondition.ToObjectReturnType0.LhsFieldType.ReferenceFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumnName();

                @JsProperty
                void setColumnName(String columnName);
            }

            @JsOverlay
            static CompareCondition.ToObjectReturnType0.LhsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            CompareCondition.ToObjectReturnType0.LhsFieldType.LiteralFieldType getLiteral();

            @JsProperty
            CompareCondition.ToObjectReturnType0.LhsFieldType.ReferenceFieldType getReference();

            @JsProperty
            void setLiteral(CompareCondition.ToObjectReturnType0.LhsFieldType.LiteralFieldType literal);

            @JsProperty
            void setReference(
                    CompareCondition.ToObjectReturnType0.LhsFieldType.ReferenceFieldType reference);
        }

        @JsOverlay
        static CompareCondition.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getCaseSensitivity();

        @JsProperty
        CompareCondition.ToObjectReturnType0.LhsFieldType getLhs();

        @JsProperty
        double getOperation();

        @JsProperty
        Object getRhs();

        @JsProperty
        void setCaseSensitivity(double caseSensitivity);

        @JsProperty
        void setLhs(CompareCondition.ToObjectReturnType0.LhsFieldType lhs);

        @JsProperty
        void setOperation(double operation);

        @JsProperty
        void setRhs(Object rhs);
    }

    public static CompareOperationMap CompareOperation;

    public static native CompareCondition deserializeBinary(Uint8Array bytes);

    public static native CompareCondition deserializeBinaryFromReader(
            CompareCondition message, Object reader);

    public static native void serializeBinaryToWriter(CompareCondition message, Object writer);

    public static native CompareCondition.ToObjectReturnType toObject(
            boolean includeInstance, CompareCondition msg);

    public native void clearLhs();

    public native void clearRhs();

    public native double getCaseSensitivity();

    public native Value getLhs();

    public native double getOperation();

    public native Value getRhs();

    public native boolean hasLhs();

    public native boolean hasRhs();

    public native Uint8Array serializeBinary();

    public native void setCaseSensitivity(double value);

    public native void setLhs();

    public native void setLhs(Value value);

    public native void setOperation(double value);

    public native void setRhs();

    public native void setRhs(Value value);

    public native CompareCondition.ToObjectReturnType0 toObject();

    public native CompareCondition.ToObjectReturnType0 toObject(boolean includeInstance);
}
