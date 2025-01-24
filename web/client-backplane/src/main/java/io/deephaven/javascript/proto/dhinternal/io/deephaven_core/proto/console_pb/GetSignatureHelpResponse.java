//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb;

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
        name = "dhinternal.io.deephaven_core.proto.console_pb.GetSignatureHelpResponse",
        namespace = JsPackage.GLOBAL)
public class GetSignatureHelpResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SignaturesListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface DocumentationFieldType {
                @JsOverlay
                static GetSignatureHelpResponse.ToObjectReturnType.SignaturesListFieldType.DocumentationFieldType create() {
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

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ParametersListFieldType {
                @JsOverlay
                static GetSignatureHelpResponse.ToObjectReturnType.SignaturesListFieldType.ParametersListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getDocumentation();

                @JsProperty
                String getLabel();

                @JsProperty
                void setDocumentation(Object documentation);

                @JsProperty
                void setLabel(String label);
            }

            @JsOverlay
            static GetSignatureHelpResponse.ToObjectReturnType.SignaturesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getActiveParameter();

            @JsProperty
            GetSignatureHelpResponse.ToObjectReturnType.SignaturesListFieldType.DocumentationFieldType getDocumentation();

            @JsProperty
            String getLabel();

            @JsProperty
            JsArray<GetSignatureHelpResponse.ToObjectReturnType.SignaturesListFieldType.ParametersListFieldType> getParametersList();

            @JsProperty
            void setActiveParameter(double activeParameter);

            @JsProperty
            void setDocumentation(
                    GetSignatureHelpResponse.ToObjectReturnType.SignaturesListFieldType.DocumentationFieldType documentation);

            @JsProperty
            void setLabel(String label);

            @JsProperty
            void setParametersList(
                    JsArray<GetSignatureHelpResponse.ToObjectReturnType.SignaturesListFieldType.ParametersListFieldType> parametersList);

            @JsOverlay
            default void setParametersList(
                    GetSignatureHelpResponse.ToObjectReturnType.SignaturesListFieldType.ParametersListFieldType[] parametersList) {
                setParametersList(
                        Js.<JsArray<GetSignatureHelpResponse.ToObjectReturnType.SignaturesListFieldType.ParametersListFieldType>>uncheckedCast(
                                parametersList));
            }
        }

        @JsOverlay
        static GetSignatureHelpResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getActiveParameter();

        @JsProperty
        double getActiveSignature();

        @JsProperty
        JsArray<GetSignatureHelpResponse.ToObjectReturnType.SignaturesListFieldType> getSignaturesList();

        @JsProperty
        void setActiveParameter(double activeParameter);

        @JsProperty
        void setActiveSignature(double activeSignature);

        @JsProperty
        void setSignaturesList(
                JsArray<GetSignatureHelpResponse.ToObjectReturnType.SignaturesListFieldType> signaturesList);

        @JsOverlay
        default void setSignaturesList(
                GetSignatureHelpResponse.ToObjectReturnType.SignaturesListFieldType[] signaturesList) {
            setSignaturesList(
                    Js.<JsArray<GetSignatureHelpResponse.ToObjectReturnType.SignaturesListFieldType>>uncheckedCast(
                            signaturesList));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SignaturesListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface DocumentationFieldType {
                @JsOverlay
                static GetSignatureHelpResponse.ToObjectReturnType0.SignaturesListFieldType.DocumentationFieldType create() {
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

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ParametersListFieldType {
                @JsOverlay
                static GetSignatureHelpResponse.ToObjectReturnType0.SignaturesListFieldType.ParametersListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getDocumentation();

                @JsProperty
                String getLabel();

                @JsProperty
                void setDocumentation(Object documentation);

                @JsProperty
                void setLabel(String label);
            }

            @JsOverlay
            static GetSignatureHelpResponse.ToObjectReturnType0.SignaturesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getActiveParameter();

            @JsProperty
            GetSignatureHelpResponse.ToObjectReturnType0.SignaturesListFieldType.DocumentationFieldType getDocumentation();

            @JsProperty
            String getLabel();

            @JsProperty
            JsArray<GetSignatureHelpResponse.ToObjectReturnType0.SignaturesListFieldType.ParametersListFieldType> getParametersList();

            @JsProperty
            void setActiveParameter(double activeParameter);

            @JsProperty
            void setDocumentation(
                    GetSignatureHelpResponse.ToObjectReturnType0.SignaturesListFieldType.DocumentationFieldType documentation);

            @JsProperty
            void setLabel(String label);

            @JsProperty
            void setParametersList(
                    JsArray<GetSignatureHelpResponse.ToObjectReturnType0.SignaturesListFieldType.ParametersListFieldType> parametersList);

            @JsOverlay
            default void setParametersList(
                    GetSignatureHelpResponse.ToObjectReturnType0.SignaturesListFieldType.ParametersListFieldType[] parametersList) {
                setParametersList(
                        Js.<JsArray<GetSignatureHelpResponse.ToObjectReturnType0.SignaturesListFieldType.ParametersListFieldType>>uncheckedCast(
                                parametersList));
            }
        }

        @JsOverlay
        static GetSignatureHelpResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getActiveParameter();

        @JsProperty
        double getActiveSignature();

        @JsProperty
        JsArray<GetSignatureHelpResponse.ToObjectReturnType0.SignaturesListFieldType> getSignaturesList();

        @JsProperty
        void setActiveParameter(double activeParameter);

        @JsProperty
        void setActiveSignature(double activeSignature);

        @JsProperty
        void setSignaturesList(
                JsArray<GetSignatureHelpResponse.ToObjectReturnType0.SignaturesListFieldType> signaturesList);

        @JsOverlay
        default void setSignaturesList(
                GetSignatureHelpResponse.ToObjectReturnType0.SignaturesListFieldType[] signaturesList) {
            setSignaturesList(
                    Js.<JsArray<GetSignatureHelpResponse.ToObjectReturnType0.SignaturesListFieldType>>uncheckedCast(
                            signaturesList));
        }
    }

    public static native GetSignatureHelpResponse deserializeBinary(Uint8Array bytes);

    public static native GetSignatureHelpResponse deserializeBinaryFromReader(
            GetSignatureHelpResponse message, Object reader);

    public static native void serializeBinaryToWriter(
            GetSignatureHelpResponse message, Object writer);

    public static native GetSignatureHelpResponse.ToObjectReturnType toObject(
            boolean includeInstance, GetSignatureHelpResponse msg);

    public native SignatureInformation addSignatures();

    public native SignatureInformation addSignatures(SignatureInformation value, double index);

    public native SignatureInformation addSignatures(SignatureInformation value);

    public native void clearActiveParameter();

    public native void clearActiveSignature();

    public native void clearSignaturesList();

    public native int getActiveParameter();

    public native int getActiveSignature();

    public native JsArray<SignatureInformation> getSignaturesList();

    public native boolean hasActiveParameter();

    public native boolean hasActiveSignature();

    public native Uint8Array serializeBinary();

    public native void setActiveParameter(int value);

    public native void setActiveSignature(int value);

    public native void setSignaturesList(JsArray<SignatureInformation> value);

    @JsOverlay
    public final void setSignaturesList(SignatureInformation[] value) {
        setSignaturesList(Js.<JsArray<SignatureInformation>>uncheckedCast(value));
    }

    public native GetSignatureHelpResponse.ToObjectReturnType0 toObject();

    public native GetSignatureHelpResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
