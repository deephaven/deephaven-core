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
        name = "dhinternal.io.deephaven_core.proto.console_pb.SignatureHelpContext",
        namespace = JsPackage.GLOBAL)
public class SignatureHelpContext {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ActiveSignatureHelpFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SignaturesListFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface DocumentationFieldType {
                    @JsOverlay
                    static SignatureHelpContext.ToObjectReturnType.ActiveSignatureHelpFieldType.SignaturesListFieldType.DocumentationFieldType create() {
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
                    static SignatureHelpContext.ToObjectReturnType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType create() {
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
                static SignatureHelpContext.ToObjectReturnType.ActiveSignatureHelpFieldType.SignaturesListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getActiveParameter();

                @JsProperty
                SignatureHelpContext.ToObjectReturnType.ActiveSignatureHelpFieldType.SignaturesListFieldType.DocumentationFieldType getDocumentation();

                @JsProperty
                String getLabel();

                @JsProperty
                JsArray<SignatureHelpContext.ToObjectReturnType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType> getParametersList();

                @JsProperty
                void setActiveParameter(double activeParameter);

                @JsProperty
                void setDocumentation(
                        SignatureHelpContext.ToObjectReturnType.ActiveSignatureHelpFieldType.SignaturesListFieldType.DocumentationFieldType documentation);

                @JsProperty
                void setLabel(String label);

                @JsProperty
                void setParametersList(
                        JsArray<SignatureHelpContext.ToObjectReturnType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType> parametersList);

                @JsOverlay
                default void setParametersList(
                        SignatureHelpContext.ToObjectReturnType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType[] parametersList) {
                    setParametersList(
                            Js.<JsArray<SignatureHelpContext.ToObjectReturnType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType>>uncheckedCast(
                                    parametersList));
                }
            }

            @JsOverlay
            static SignatureHelpContext.ToObjectReturnType.ActiveSignatureHelpFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getActiveParameter();

            @JsProperty
            double getActiveSignature();

            @JsProperty
            JsArray<SignatureHelpContext.ToObjectReturnType.ActiveSignatureHelpFieldType.SignaturesListFieldType> getSignaturesList();

            @JsProperty
            void setActiveParameter(double activeParameter);

            @JsProperty
            void setActiveSignature(double activeSignature);

            @JsProperty
            void setSignaturesList(
                    JsArray<SignatureHelpContext.ToObjectReturnType.ActiveSignatureHelpFieldType.SignaturesListFieldType> signaturesList);

            @JsOverlay
            default void setSignaturesList(
                    SignatureHelpContext.ToObjectReturnType.ActiveSignatureHelpFieldType.SignaturesListFieldType[] signaturesList) {
                setSignaturesList(
                        Js.<JsArray<SignatureHelpContext.ToObjectReturnType.ActiveSignatureHelpFieldType.SignaturesListFieldType>>uncheckedCast(
                                signaturesList));
            }
        }

        @JsOverlay
        static SignatureHelpContext.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        SignatureHelpContext.ToObjectReturnType.ActiveSignatureHelpFieldType getActiveSignatureHelp();

        @JsProperty
        String getTriggerCharacter();

        @JsProperty
        double getTriggerKind();

        @JsProperty
        boolean isIsRetrigger();

        @JsProperty
        void setActiveSignatureHelp(
                SignatureHelpContext.ToObjectReturnType.ActiveSignatureHelpFieldType activeSignatureHelp);

        @JsProperty
        void setIsRetrigger(boolean isRetrigger);

        @JsProperty
        void setTriggerCharacter(String triggerCharacter);

        @JsProperty
        void setTriggerKind(double triggerKind);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ActiveSignatureHelpFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SignaturesListFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface DocumentationFieldType {
                    @JsOverlay
                    static SignatureHelpContext.ToObjectReturnType0.ActiveSignatureHelpFieldType.SignaturesListFieldType.DocumentationFieldType create() {
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
                    static SignatureHelpContext.ToObjectReturnType0.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType create() {
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
                static SignatureHelpContext.ToObjectReturnType0.ActiveSignatureHelpFieldType.SignaturesListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getActiveParameter();

                @JsProperty
                SignatureHelpContext.ToObjectReturnType0.ActiveSignatureHelpFieldType.SignaturesListFieldType.DocumentationFieldType getDocumentation();

                @JsProperty
                String getLabel();

                @JsProperty
                JsArray<SignatureHelpContext.ToObjectReturnType0.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType> getParametersList();

                @JsProperty
                void setActiveParameter(double activeParameter);

                @JsProperty
                void setDocumentation(
                        SignatureHelpContext.ToObjectReturnType0.ActiveSignatureHelpFieldType.SignaturesListFieldType.DocumentationFieldType documentation);

                @JsProperty
                void setLabel(String label);

                @JsProperty
                void setParametersList(
                        JsArray<SignatureHelpContext.ToObjectReturnType0.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType> parametersList);

                @JsOverlay
                default void setParametersList(
                        SignatureHelpContext.ToObjectReturnType0.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType[] parametersList) {
                    setParametersList(
                            Js.<JsArray<SignatureHelpContext.ToObjectReturnType0.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType>>uncheckedCast(
                                    parametersList));
                }
            }

            @JsOverlay
            static SignatureHelpContext.ToObjectReturnType0.ActiveSignatureHelpFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getActiveParameter();

            @JsProperty
            double getActiveSignature();

            @JsProperty
            JsArray<SignatureHelpContext.ToObjectReturnType0.ActiveSignatureHelpFieldType.SignaturesListFieldType> getSignaturesList();

            @JsProperty
            void setActiveParameter(double activeParameter);

            @JsProperty
            void setActiveSignature(double activeSignature);

            @JsProperty
            void setSignaturesList(
                    JsArray<SignatureHelpContext.ToObjectReturnType0.ActiveSignatureHelpFieldType.SignaturesListFieldType> signaturesList);

            @JsOverlay
            default void setSignaturesList(
                    SignatureHelpContext.ToObjectReturnType0.ActiveSignatureHelpFieldType.SignaturesListFieldType[] signaturesList) {
                setSignaturesList(
                        Js.<JsArray<SignatureHelpContext.ToObjectReturnType0.ActiveSignatureHelpFieldType.SignaturesListFieldType>>uncheckedCast(
                                signaturesList));
            }
        }

        @JsOverlay
        static SignatureHelpContext.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        SignatureHelpContext.ToObjectReturnType0.ActiveSignatureHelpFieldType getActiveSignatureHelp();

        @JsProperty
        String getTriggerCharacter();

        @JsProperty
        double getTriggerKind();

        @JsProperty
        boolean isIsRetrigger();

        @JsProperty
        void setActiveSignatureHelp(
                SignatureHelpContext.ToObjectReturnType0.ActiveSignatureHelpFieldType activeSignatureHelp);

        @JsProperty
        void setIsRetrigger(boolean isRetrigger);

        @JsProperty
        void setTriggerCharacter(String triggerCharacter);

        @JsProperty
        void setTriggerKind(double triggerKind);
    }

    public static native SignatureHelpContext deserializeBinary(Uint8Array bytes);

    public static native SignatureHelpContext deserializeBinaryFromReader(
            SignatureHelpContext message, Object reader);

    public static native void serializeBinaryToWriter(SignatureHelpContext message, Object writer);

    public static native SignatureHelpContext.ToObjectReturnType toObject(
            boolean includeInstance, SignatureHelpContext msg);

    public native void clearActiveSignatureHelp();

    public native void clearTriggerCharacter();

    public native GetSignatureHelpResponse getActiveSignatureHelp();

    public native boolean getIsRetrigger();

    public native String getTriggerCharacter();

    public native int getTriggerKind();

    public native boolean hasActiveSignatureHelp();

    public native boolean hasTriggerCharacter();

    public native Uint8Array serializeBinary();

    public native void setActiveSignatureHelp();

    public native void setActiveSignatureHelp(GetSignatureHelpResponse value);

    public native void setIsRetrigger(boolean value);

    public native void setTriggerCharacter(String value);

    public native void setTriggerKind(int value);

    public native SignatureHelpContext.ToObjectReturnType0 toObject();

    public native SignatureHelpContext.ToObjectReturnType0 toObject(boolean includeInstance);
}
