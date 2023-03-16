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
        name = "dhinternal.io.deephaven.proto.console_pb.GetSignatureHelpRequest",
        namespace = JsPackage.GLOBAL)
public class GetSignatureHelpRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ContextFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ActiveSignatureHelpFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface SignaturesListFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface ParametersListFieldType {
                        @JsOverlay
                        static GetSignatureHelpRequest.ToObjectReturnType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType create() {
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
                    static GetSignatureHelpRequest.ToObjectReturnType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    double getActiveParameter();

                    @JsProperty
                    String getDocumentation();

                    @JsProperty
                    String getLabel();

                    @JsProperty
                    JsArray<GetSignatureHelpRequest.ToObjectReturnType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType> getParametersList();

                    @JsProperty
                    void setActiveParameter(double activeParameter);

                    @JsProperty
                    void setDocumentation(String documentation);

                    @JsProperty
                    void setLabel(String label);

                    @JsProperty
                    void setParametersList(
                            JsArray<GetSignatureHelpRequest.ToObjectReturnType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType> parametersList);

                    @JsOverlay
                    default void setParametersList(
                            GetSignatureHelpRequest.ToObjectReturnType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType[] parametersList) {
                        setParametersList(
                                Js.<JsArray<GetSignatureHelpRequest.ToObjectReturnType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType>>uncheckedCast(
                                        parametersList));
                    }
                }

                @JsOverlay
                static GetSignatureHelpRequest.ToObjectReturnType.ContextFieldType.ActiveSignatureHelpFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getActiveParameter();

                @JsProperty
                double getActiveSignature();

                @JsProperty
                JsArray<GetSignatureHelpRequest.ToObjectReturnType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType> getSignaturesList();

                @JsProperty
                void setActiveParameter(double activeParameter);

                @JsProperty
                void setActiveSignature(double activeSignature);

                @JsProperty
                void setSignaturesList(
                        JsArray<GetSignatureHelpRequest.ToObjectReturnType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType> signaturesList);

                @JsOverlay
                default void setSignaturesList(
                        GetSignatureHelpRequest.ToObjectReturnType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType[] signaturesList) {
                    setSignaturesList(
                            Js.<JsArray<GetSignatureHelpRequest.ToObjectReturnType.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType>>uncheckedCast(
                                    signaturesList));
                }
            }

            @JsOverlay
            static GetSignatureHelpRequest.ToObjectReturnType.ContextFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            GetSignatureHelpRequest.ToObjectReturnType.ContextFieldType.ActiveSignatureHelpFieldType getActiveSignatureHelp();

            @JsProperty
            String getTriggerCharacter();

            @JsProperty
            double getTriggerKind();

            @JsProperty
            boolean isIsRetrigger();

            @JsProperty
            void setActiveSignatureHelp(
                    GetSignatureHelpRequest.ToObjectReturnType.ContextFieldType.ActiveSignatureHelpFieldType activeSignatureHelp);

            @JsProperty
            void setIsRetrigger(boolean isRetrigger);

            @JsProperty
            void setTriggerCharacter(String triggerCharacter);

            @JsProperty
            void setTriggerKind(double triggerKind);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface PositionFieldType {
            @JsOverlay
            static GetSignatureHelpRequest.ToObjectReturnType.PositionFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCharacter();

            @JsProperty
            double getLine();

            @JsProperty
            void setCharacter(double character);

            @JsProperty
            void setLine(double line);
        }

        @JsOverlay
        static GetSignatureHelpRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        GetSignatureHelpRequest.ToObjectReturnType.ContextFieldType getContext();

        @JsProperty
        GetSignatureHelpRequest.ToObjectReturnType.PositionFieldType getPosition();

        @JsProperty
        void setContext(GetSignatureHelpRequest.ToObjectReturnType.ContextFieldType context);

        @JsProperty
        void setPosition(GetSignatureHelpRequest.ToObjectReturnType.PositionFieldType position);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ContextFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ActiveSignatureHelpFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface SignaturesListFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface ParametersListFieldType {
                        @JsOverlay
                        static GetSignatureHelpRequest.ToObjectReturnType0.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType create() {
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
                    static GetSignatureHelpRequest.ToObjectReturnType0.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    double getActiveParameter();

                    @JsProperty
                    String getDocumentation();

                    @JsProperty
                    String getLabel();

                    @JsProperty
                    JsArray<GetSignatureHelpRequest.ToObjectReturnType0.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType> getParametersList();

                    @JsProperty
                    void setActiveParameter(double activeParameter);

                    @JsProperty
                    void setDocumentation(String documentation);

                    @JsProperty
                    void setLabel(String label);

                    @JsProperty
                    void setParametersList(
                            JsArray<GetSignatureHelpRequest.ToObjectReturnType0.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType> parametersList);

                    @JsOverlay
                    default void setParametersList(
                            GetSignatureHelpRequest.ToObjectReturnType0.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType[] parametersList) {
                        setParametersList(
                                Js.<JsArray<GetSignatureHelpRequest.ToObjectReturnType0.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType.ParametersListFieldType>>uncheckedCast(
                                        parametersList));
                    }
                }

                @JsOverlay
                static GetSignatureHelpRequest.ToObjectReturnType0.ContextFieldType.ActiveSignatureHelpFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getActiveParameter();

                @JsProperty
                double getActiveSignature();

                @JsProperty
                JsArray<GetSignatureHelpRequest.ToObjectReturnType0.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType> getSignaturesList();

                @JsProperty
                void setActiveParameter(double activeParameter);

                @JsProperty
                void setActiveSignature(double activeSignature);

                @JsProperty
                void setSignaturesList(
                        JsArray<GetSignatureHelpRequest.ToObjectReturnType0.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType> signaturesList);

                @JsOverlay
                default void setSignaturesList(
                        GetSignatureHelpRequest.ToObjectReturnType0.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType[] signaturesList) {
                    setSignaturesList(
                            Js.<JsArray<GetSignatureHelpRequest.ToObjectReturnType0.ContextFieldType.ActiveSignatureHelpFieldType.SignaturesListFieldType>>uncheckedCast(
                                    signaturesList));
                }
            }

            @JsOverlay
            static GetSignatureHelpRequest.ToObjectReturnType0.ContextFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            GetSignatureHelpRequest.ToObjectReturnType0.ContextFieldType.ActiveSignatureHelpFieldType getActiveSignatureHelp();

            @JsProperty
            String getTriggerCharacter();

            @JsProperty
            double getTriggerKind();

            @JsProperty
            boolean isIsRetrigger();

            @JsProperty
            void setActiveSignatureHelp(
                    GetSignatureHelpRequest.ToObjectReturnType0.ContextFieldType.ActiveSignatureHelpFieldType activeSignatureHelp);

            @JsProperty
            void setIsRetrigger(boolean isRetrigger);

            @JsProperty
            void setTriggerCharacter(String triggerCharacter);

            @JsProperty
            void setTriggerKind(double triggerKind);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface PositionFieldType {
            @JsOverlay
            static GetSignatureHelpRequest.ToObjectReturnType0.PositionFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCharacter();

            @JsProperty
            double getLine();

            @JsProperty
            void setCharacter(double character);

            @JsProperty
            void setLine(double line);
        }

        @JsOverlay
        static GetSignatureHelpRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        GetSignatureHelpRequest.ToObjectReturnType0.ContextFieldType getContext();

        @JsProperty
        GetSignatureHelpRequest.ToObjectReturnType0.PositionFieldType getPosition();

        @JsProperty
        void setContext(GetSignatureHelpRequest.ToObjectReturnType0.ContextFieldType context);

        @JsProperty
        void setPosition(GetSignatureHelpRequest.ToObjectReturnType0.PositionFieldType position);
    }

    public static native GetSignatureHelpRequest deserializeBinary(Uint8Array bytes);

    public static native GetSignatureHelpRequest deserializeBinaryFromReader(
            GetSignatureHelpRequest message, Object reader);

    public static native void serializeBinaryToWriter(GetSignatureHelpRequest message, Object writer);

    public static native GetSignatureHelpRequest.ToObjectReturnType toObject(
            boolean includeInstance, GetSignatureHelpRequest msg);

    public native void clearContext();

    public native void clearPosition();

    public native SignatureHelpContext getContext();

    public native Position getPosition();

    public native boolean hasContext();

    public native boolean hasPosition();

    public native Uint8Array serializeBinary();

    public native void setContext();

    public native void setContext(SignatureHelpContext value);

    public native void setPosition();

    public native void setPosition(Position value);

    public native GetSignatureHelpRequest.ToObjectReturnType0 toObject();

    public native GetSignatureHelpRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
