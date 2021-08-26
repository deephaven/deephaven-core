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
        name = "dhinternal.io.deephaven.proto.table_pb.Value",
        namespace = JsPackage.GLOBAL)
public class Value {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface LiteralFieldType {
            @JsOverlay
            static Value.ToObjectReturnType.LiteralFieldType create() {
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
            static Value.ToObjectReturnType.ReferenceFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getColumnName();

            @JsProperty
            void setColumnName(String columnName);
        }

        @JsOverlay
        static Value.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Value.ToObjectReturnType.LiteralFieldType getLiteral();

        @JsProperty
        Value.ToObjectReturnType.ReferenceFieldType getReference();

        @JsProperty
        void setLiteral(Value.ToObjectReturnType.LiteralFieldType literal);

        @JsProperty
        void setReference(Value.ToObjectReturnType.ReferenceFieldType reference);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface LiteralFieldType {
            @JsOverlay
            static Value.ToObjectReturnType0.LiteralFieldType create() {
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
            static Value.ToObjectReturnType0.ReferenceFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getColumnName();

            @JsProperty
            void setColumnName(String columnName);
        }

        @JsOverlay
        static Value.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Value.ToObjectReturnType0.LiteralFieldType getLiteral();

        @JsProperty
        Value.ToObjectReturnType0.ReferenceFieldType getReference();

        @JsProperty
        void setLiteral(Value.ToObjectReturnType0.LiteralFieldType literal);

        @JsProperty
        void setReference(Value.ToObjectReturnType0.ReferenceFieldType reference);
    }

    public static native Value deserializeBinary(Uint8Array bytes);

    public static native Value deserializeBinaryFromReader(Value message, Object reader);

    public static native void serializeBinaryToWriter(Value message, Object writer);

    public static native Value.ToObjectReturnType toObject(boolean includeInstance, Value msg);

    public native void clearLiteral();

    public native void clearReference();

    public native int getDataCase();

    public native Literal getLiteral();

    public native Reference getReference();

    public native boolean hasLiteral();

    public native boolean hasReference();

    public native Uint8Array serializeBinary();

    public native void setLiteral();

    public native void setLiteral(Literal value);

    public native void setReference();

    public native void setReference(Reference value);

    public native Value.ToObjectReturnType0 toObject();

    public native Value.ToObjectReturnType0 toObject(boolean includeInstance);
}
