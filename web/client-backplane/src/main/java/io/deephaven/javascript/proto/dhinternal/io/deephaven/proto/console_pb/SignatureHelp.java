/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

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
        name = "dhinternal.io.deephaven.proto.console_pb.SignatureHelp",
        namespace = JsPackage.GLOBAL)
public class SignatureHelp {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SignaturesListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ParametersListFieldType {
                @JsOverlay
                static SignatureHelp.ToObjectReturnType.SignaturesListFieldType.ParametersListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getDocumentation();

                @JsProperty
                String getLabel();

                @JsProperty
                void setDocumentation(String documentation);

                @JsProperty
                void setLabel(String label);
            }

            @JsOverlay
            static SignatureHelp.ToObjectReturnType.SignaturesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getActiveParameter();

            @JsProperty
            String getDocumentation();

            @JsProperty
            String getLabel();

            @JsProperty
            JsArray<SignatureHelp.ToObjectReturnType.SignaturesListFieldType.ParametersListFieldType> getParametersList();

            @JsProperty
            void setActiveParameter(double activeParameter);

            @JsProperty
            void setDocumentation(String documentation);

            @JsProperty
            void setLabel(String label);

            @JsProperty
            void setParametersList(
                    JsArray<SignatureHelp.ToObjectReturnType.SignaturesListFieldType.ParametersListFieldType> parametersList);

            @JsOverlay
            default void setParametersList(
                    SignatureHelp.ToObjectReturnType.SignaturesListFieldType.ParametersListFieldType[] parametersList) {
                setParametersList(
                        Js.<JsArray<SignatureHelp.ToObjectReturnType.SignaturesListFieldType.ParametersListFieldType>>uncheckedCast(
                                parametersList));
            }
        }

        @JsOverlay
        static SignatureHelp.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getActiveParameter();

        @JsProperty
        double getActiveSignature();

        @JsProperty
        JsArray<SignatureHelp.ToObjectReturnType.SignaturesListFieldType> getSignaturesList();

        @JsProperty
        void setActiveParameter(double activeParameter);

        @JsProperty
        void setActiveSignature(double activeSignature);

        @JsProperty
        void setSignaturesList(
                JsArray<SignatureHelp.ToObjectReturnType.SignaturesListFieldType> signaturesList);

        @JsOverlay
        default void setSignaturesList(
                SignatureHelp.ToObjectReturnType.SignaturesListFieldType[] signaturesList) {
            setSignaturesList(
                    Js.<JsArray<SignatureHelp.ToObjectReturnType.SignaturesListFieldType>>uncheckedCast(
                            signaturesList));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SignaturesListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ParametersListFieldType {
                @JsOverlay
                static SignatureHelp.ToObjectReturnType0.SignaturesListFieldType.ParametersListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getDocumentation();

                @JsProperty
                String getLabel();

                @JsProperty
                void setDocumentation(String documentation);

                @JsProperty
                void setLabel(String label);
            }

            @JsOverlay
            static SignatureHelp.ToObjectReturnType0.SignaturesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getActiveParameter();

            @JsProperty
            String getDocumentation();

            @JsProperty
            String getLabel();

            @JsProperty
            JsArray<SignatureHelp.ToObjectReturnType0.SignaturesListFieldType.ParametersListFieldType> getParametersList();

            @JsProperty
            void setActiveParameter(double activeParameter);

            @JsProperty
            void setDocumentation(String documentation);

            @JsProperty
            void setLabel(String label);

            @JsProperty
            void setParametersList(
                    JsArray<SignatureHelp.ToObjectReturnType0.SignaturesListFieldType.ParametersListFieldType> parametersList);

            @JsOverlay
            default void setParametersList(
                    SignatureHelp.ToObjectReturnType0.SignaturesListFieldType.ParametersListFieldType[] parametersList) {
                setParametersList(
                        Js.<JsArray<SignatureHelp.ToObjectReturnType0.SignaturesListFieldType.ParametersListFieldType>>uncheckedCast(
                                parametersList));
            }
        }

        @JsOverlay
        static SignatureHelp.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getActiveParameter();

        @JsProperty
        double getActiveSignature();

        @JsProperty
        JsArray<SignatureHelp.ToObjectReturnType0.SignaturesListFieldType> getSignaturesList();

        @JsProperty
        void setActiveParameter(double activeParameter);

        @JsProperty
        void setActiveSignature(double activeSignature);

        @JsProperty
        void setSignaturesList(
                JsArray<SignatureHelp.ToObjectReturnType0.SignaturesListFieldType> signaturesList);

        @JsOverlay
        default void setSignaturesList(
                SignatureHelp.ToObjectReturnType0.SignaturesListFieldType[] signaturesList) {
            setSignaturesList(
                    Js.<JsArray<SignatureHelp.ToObjectReturnType0.SignaturesListFieldType>>uncheckedCast(
                            signaturesList));
        }
    }

    public static native SignatureHelp deserializeBinary(Uint8Array bytes);

    public static native SignatureHelp deserializeBinaryFromReader(
            SignatureHelp message, Object reader);

    public static native void serializeBinaryToWriter(SignatureHelp message, Object writer);

    public static native SignatureHelp.ToObjectReturnType toObject(
            boolean includeInstance, SignatureHelp msg);

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

    public native SignatureHelp.ToObjectReturnType0 toObject();

    public native SignatureHelp.ToObjectReturnType0 toObject(boolean includeInstance);
}
