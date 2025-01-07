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
        name = "dhinternal.io.deephaven_core.proto.console_pb.GetPublishDiagnosticResponse",
        namespace = JsPackage.GLOBAL)
public class GetPublishDiagnosticResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DiagnosticsListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CodeDescriptionFieldType {
                @JsOverlay
                static GetPublishDiagnosticResponse.ToObjectReturnType.DiagnosticsListFieldType.CodeDescriptionFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getHref();

                @JsProperty
                void setHref(String href);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetDataUnionType {
                @JsOverlay
                static GetPublishDiagnosticResponse.ToObjectReturnType.DiagnosticsListFieldType.GetDataUnionType of(
                        Object o) {
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
            public interface RangeFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface StartFieldType {
                    @JsOverlay
                    static GetPublishDiagnosticResponse.ToObjectReturnType.DiagnosticsListFieldType.RangeFieldType.StartFieldType create() {
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
                static GetPublishDiagnosticResponse.ToObjectReturnType.DiagnosticsListFieldType.RangeFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getEnd();

                @JsProperty
                GetPublishDiagnosticResponse.ToObjectReturnType.DiagnosticsListFieldType.RangeFieldType.StartFieldType getStart();

                @JsProperty
                void setEnd(Object end);

                @JsProperty
                void setStart(
                        GetPublishDiagnosticResponse.ToObjectReturnType.DiagnosticsListFieldType.RangeFieldType.StartFieldType start);
            }

            @JsOverlay
            static GetPublishDiagnosticResponse.ToObjectReturnType.DiagnosticsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getCode();

            @JsProperty
            GetPublishDiagnosticResponse.ToObjectReturnType.DiagnosticsListFieldType.CodeDescriptionFieldType getCodeDescription();

            @JsProperty
            GetPublishDiagnosticResponse.ToObjectReturnType.DiagnosticsListFieldType.GetDataUnionType getData();

            @JsProperty
            String getMessage();

            @JsProperty
            GetPublishDiagnosticResponse.ToObjectReturnType.DiagnosticsListFieldType.RangeFieldType getRange();

            @JsProperty
            double getSeverity();

            @JsProperty
            String getSource();

            @JsProperty
            JsArray<Double> getTagsList();

            @JsProperty
            void setCode(String code);

            @JsProperty
            void setCodeDescription(
                    GetPublishDiagnosticResponse.ToObjectReturnType.DiagnosticsListFieldType.CodeDescriptionFieldType codeDescription);

            @JsProperty
            void setData(
                    GetPublishDiagnosticResponse.ToObjectReturnType.DiagnosticsListFieldType.GetDataUnionType data);

            @JsOverlay
            default void setData(String data) {
                setData(
                        Js.<GetPublishDiagnosticResponse.ToObjectReturnType.DiagnosticsListFieldType.GetDataUnionType>uncheckedCast(
                                data));
            }

            @JsOverlay
            default void setData(Uint8Array data) {
                setData(
                        Js.<GetPublishDiagnosticResponse.ToObjectReturnType.DiagnosticsListFieldType.GetDataUnionType>uncheckedCast(
                                data));
            }

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setRange(
                    GetPublishDiagnosticResponse.ToObjectReturnType.DiagnosticsListFieldType.RangeFieldType range);

            @JsProperty
            void setSeverity(double severity);

            @JsProperty
            void setSource(String source);

            @JsProperty
            void setTagsList(JsArray<Double> tagsList);

            @JsOverlay
            default void setTagsList(double[] tagsList) {
                setTagsList(Js.<JsArray<Double>>uncheckedCast(tagsList));
            }
        }

        @JsOverlay
        static GetPublishDiagnosticResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<GetPublishDiagnosticResponse.ToObjectReturnType.DiagnosticsListFieldType> getDiagnosticsList();

        @JsProperty
        String getUri();

        @JsProperty
        double getVersion();

        @JsOverlay
        default void setDiagnosticsList(
                GetPublishDiagnosticResponse.ToObjectReturnType.DiagnosticsListFieldType[] diagnosticsList) {
            setDiagnosticsList(
                    Js.<JsArray<GetPublishDiagnosticResponse.ToObjectReturnType.DiagnosticsListFieldType>>uncheckedCast(
                            diagnosticsList));
        }

        @JsProperty
        void setDiagnosticsList(
                JsArray<GetPublishDiagnosticResponse.ToObjectReturnType.DiagnosticsListFieldType> diagnosticsList);

        @JsProperty
        void setUri(String uri);

        @JsProperty
        void setVersion(double version);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DiagnosticsListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CodeDescriptionFieldType {
                @JsOverlay
                static GetPublishDiagnosticResponse.ToObjectReturnType0.DiagnosticsListFieldType.CodeDescriptionFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getHref();

                @JsProperty
                void setHref(String href);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetDataUnionType {
                @JsOverlay
                static GetPublishDiagnosticResponse.ToObjectReturnType0.DiagnosticsListFieldType.GetDataUnionType of(
                        Object o) {
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
            public interface RangeFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface StartFieldType {
                    @JsOverlay
                    static GetPublishDiagnosticResponse.ToObjectReturnType0.DiagnosticsListFieldType.RangeFieldType.StartFieldType create() {
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
                static GetPublishDiagnosticResponse.ToObjectReturnType0.DiagnosticsListFieldType.RangeFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getEnd();

                @JsProperty
                GetPublishDiagnosticResponse.ToObjectReturnType0.DiagnosticsListFieldType.RangeFieldType.StartFieldType getStart();

                @JsProperty
                void setEnd(Object end);

                @JsProperty
                void setStart(
                        GetPublishDiagnosticResponse.ToObjectReturnType0.DiagnosticsListFieldType.RangeFieldType.StartFieldType start);
            }

            @JsOverlay
            static GetPublishDiagnosticResponse.ToObjectReturnType0.DiagnosticsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getCode();

            @JsProperty
            GetPublishDiagnosticResponse.ToObjectReturnType0.DiagnosticsListFieldType.CodeDescriptionFieldType getCodeDescription();

            @JsProperty
            GetPublishDiagnosticResponse.ToObjectReturnType0.DiagnosticsListFieldType.GetDataUnionType getData();

            @JsProperty
            String getMessage();

            @JsProperty
            GetPublishDiagnosticResponse.ToObjectReturnType0.DiagnosticsListFieldType.RangeFieldType getRange();

            @JsProperty
            double getSeverity();

            @JsProperty
            String getSource();

            @JsProperty
            JsArray<Double> getTagsList();

            @JsProperty
            void setCode(String code);

            @JsProperty
            void setCodeDescription(
                    GetPublishDiagnosticResponse.ToObjectReturnType0.DiagnosticsListFieldType.CodeDescriptionFieldType codeDescription);

            @JsProperty
            void setData(
                    GetPublishDiagnosticResponse.ToObjectReturnType0.DiagnosticsListFieldType.GetDataUnionType data);

            @JsOverlay
            default void setData(String data) {
                setData(
                        Js.<GetPublishDiagnosticResponse.ToObjectReturnType0.DiagnosticsListFieldType.GetDataUnionType>uncheckedCast(
                                data));
            }

            @JsOverlay
            default void setData(Uint8Array data) {
                setData(
                        Js.<GetPublishDiagnosticResponse.ToObjectReturnType0.DiagnosticsListFieldType.GetDataUnionType>uncheckedCast(
                                data));
            }

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setRange(
                    GetPublishDiagnosticResponse.ToObjectReturnType0.DiagnosticsListFieldType.RangeFieldType range);

            @JsProperty
            void setSeverity(double severity);

            @JsProperty
            void setSource(String source);

            @JsProperty
            void setTagsList(JsArray<Double> tagsList);

            @JsOverlay
            default void setTagsList(double[] tagsList) {
                setTagsList(Js.<JsArray<Double>>uncheckedCast(tagsList));
            }
        }

        @JsOverlay
        static GetPublishDiagnosticResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<GetPublishDiagnosticResponse.ToObjectReturnType0.DiagnosticsListFieldType> getDiagnosticsList();

        @JsProperty
        String getUri();

        @JsProperty
        double getVersion();

        @JsOverlay
        default void setDiagnosticsList(
                GetPublishDiagnosticResponse.ToObjectReturnType0.DiagnosticsListFieldType[] diagnosticsList) {
            setDiagnosticsList(
                    Js.<JsArray<GetPublishDiagnosticResponse.ToObjectReturnType0.DiagnosticsListFieldType>>uncheckedCast(
                            diagnosticsList));
        }

        @JsProperty
        void setDiagnosticsList(
                JsArray<GetPublishDiagnosticResponse.ToObjectReturnType0.DiagnosticsListFieldType> diagnosticsList);

        @JsProperty
        void setUri(String uri);

        @JsProperty
        void setVersion(double version);
    }

    public static native GetPublishDiagnosticResponse deserializeBinary(Uint8Array bytes);

    public static native GetPublishDiagnosticResponse deserializeBinaryFromReader(
            GetPublishDiagnosticResponse message, Object reader);

    public static native void serializeBinaryToWriter(
            GetPublishDiagnosticResponse message, Object writer);

    public static native GetPublishDiagnosticResponse.ToObjectReturnType toObject(
            boolean includeInstance, GetPublishDiagnosticResponse msg);

    public native Diagnostic addDiagnostics();

    public native Diagnostic addDiagnostics(Diagnostic value, double index);

    public native Diagnostic addDiagnostics(Diagnostic value);

    public native void clearDiagnosticsList();

    public native void clearVersion();

    public native JsArray<Diagnostic> getDiagnosticsList();

    public native String getUri();

    public native double getVersion();

    public native boolean hasVersion();

    public native Uint8Array serializeBinary();

    @JsOverlay
    public final void setDiagnosticsList(Diagnostic[] value) {
        setDiagnosticsList(Js.<JsArray<Diagnostic>>uncheckedCast(value));
    }

    public native void setDiagnosticsList(JsArray<Diagnostic> value);

    public native void setUri(String value);

    public native void setVersion(double value);

    public native GetPublishDiagnosticResponse.ToObjectReturnType0 toObject();

    public native GetPublishDiagnosticResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
