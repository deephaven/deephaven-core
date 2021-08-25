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
    name = "dhinternal.io.deephaven.proto.table_pb.ContainsCondition",
    namespace = JsPackage.GLOBAL)
public class ContainsCondition {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ReferenceFieldType {
            @JsOverlay
            static ContainsCondition.ToObjectReturnType.ReferenceFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getColumnName();

            @JsProperty
            void setColumnName(String columnName);
        }

        @JsOverlay
        static ContainsCondition.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getCaseSensitivity();

        @JsProperty
        double getMatchType();

        @JsProperty
        ContainsCondition.ToObjectReturnType.ReferenceFieldType getReference();

        @JsProperty
        String getSearchString();

        @JsProperty
        void setCaseSensitivity(double caseSensitivity);

        @JsProperty
        void setMatchType(double matchType);

        @JsProperty
        void setReference(ContainsCondition.ToObjectReturnType.ReferenceFieldType reference);

        @JsProperty
        void setSearchString(String searchString);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ReferenceFieldType {
            @JsOverlay
            static ContainsCondition.ToObjectReturnType0.ReferenceFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getColumnName();

            @JsProperty
            void setColumnName(String columnName);
        }

        @JsOverlay
        static ContainsCondition.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getCaseSensitivity();

        @JsProperty
        double getMatchType();

        @JsProperty
        ContainsCondition.ToObjectReturnType0.ReferenceFieldType getReference();

        @JsProperty
        String getSearchString();

        @JsProperty
        void setCaseSensitivity(double caseSensitivity);

        @JsProperty
        void setMatchType(double matchType);

        @JsProperty
        void setReference(ContainsCondition.ToObjectReturnType0.ReferenceFieldType reference);

        @JsProperty
        void setSearchString(String searchString);
    }

    public static native ContainsCondition deserializeBinary(Uint8Array bytes);

    public static native ContainsCondition deserializeBinaryFromReader(
        ContainsCondition message, Object reader);

    public static native void serializeBinaryToWriter(ContainsCondition message, Object writer);

    public static native ContainsCondition.ToObjectReturnType toObject(
        boolean includeInstance, ContainsCondition msg);

    public native void clearReference();

    public native double getCaseSensitivity();

    public native double getMatchType();

    public native Reference getReference();

    public native String getSearchString();

    public native boolean hasReference();

    public native Uint8Array serializeBinary();

    public native void setCaseSensitivity(double value);

    public native void setMatchType(double value);

    public native void setReference();

    public native void setReference(Reference value);

    public native void setSearchString(String value);

    public native ContainsCondition.ToObjectReturnType0 toObject();

    public native ContainsCondition.ToObjectReturnType0 toObject(boolean includeInstance);
}
