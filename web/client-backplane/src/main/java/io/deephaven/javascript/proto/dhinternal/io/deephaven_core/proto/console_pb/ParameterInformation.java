//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.console_pb.ParameterInformation",
        namespace = JsPackage.GLOBAL)
public class ParameterInformation {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DocumentationFieldType {
            @JsOverlay
            static ParameterInformation.ToObjectReturnType.DocumentationFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getKind();

            @JsProperty
            String getValue();

            @JsProperty
            void setKind(String kind);

            @JsProperty
            void setValue(String value);
        }

        @JsOverlay
        static ParameterInformation.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        ParameterInformation.ToObjectReturnType.DocumentationFieldType getDocumentation();

        @JsProperty
        String getLabel();

        @JsProperty
        void setDocumentation(
                ParameterInformation.ToObjectReturnType.DocumentationFieldType documentation);

        @JsProperty
        void setLabel(String label);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DocumentationFieldType {
            @JsOverlay
            static ParameterInformation.ToObjectReturnType0.DocumentationFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getKind();

            @JsProperty
            String getValue();

            @JsProperty
            void setKind(String kind);

            @JsProperty
            void setValue(String value);
        }

        @JsOverlay
        static ParameterInformation.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        ParameterInformation.ToObjectReturnType0.DocumentationFieldType getDocumentation();

        @JsProperty
        String getLabel();

        @JsProperty
        void setDocumentation(
                ParameterInformation.ToObjectReturnType0.DocumentationFieldType documentation);

        @JsProperty
        void setLabel(String label);
    }

    public static native ParameterInformation deserializeBinary(Uint8Array bytes);

    public static native ParameterInformation deserializeBinaryFromReader(
            ParameterInformation message, Object reader);

    public static native void serializeBinaryToWriter(ParameterInformation message, Object writer);

    public static native ParameterInformation.ToObjectReturnType toObject(
            boolean includeInstance, ParameterInformation msg);

    public native void clearDocumentation();

    public native MarkupContent getDocumentation();

    public native String getLabel();

    public native boolean hasDocumentation();

    public native Uint8Array serializeBinary();

    public native void setDocumentation();

    public native void setDocumentation(MarkupContent value);

    public native void setLabel(String value);

    public native ParameterInformation.ToObjectReturnType0 toObject();

    public native ParameterInformation.ToObjectReturnType0 toObject(boolean includeInstance);
}
