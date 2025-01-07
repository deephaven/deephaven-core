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
        name = "dhinternal.io.deephaven_core.proto.console_pb.GetPullDiagnosticResponse",
        namespace = JsPackage.GLOBAL)
public class GetPullDiagnosticResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ItemsListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CodeDescriptionFieldType {
                @JsOverlay
                static GetPullDiagnosticResponse.ToObjectReturnType.ItemsListFieldType.CodeDescriptionFieldType create() {
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
                static GetPullDiagnosticResponse.ToObjectReturnType.ItemsListFieldType.GetDataUnionType of(
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
                    static GetPullDiagnosticResponse.ToObjectReturnType.ItemsListFieldType.RangeFieldType.StartFieldType create() {
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
                static GetPullDiagnosticResponse.ToObjectReturnType.ItemsListFieldType.RangeFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getEnd();

                @JsProperty
                GetPullDiagnosticResponse.ToObjectReturnType.ItemsListFieldType.RangeFieldType.StartFieldType getStart();

                @JsProperty
                void setEnd(Object end);

                @JsProperty
                void setStart(
                        GetPullDiagnosticResponse.ToObjectReturnType.ItemsListFieldType.RangeFieldType.StartFieldType start);
            }

            @JsOverlay
            static GetPullDiagnosticResponse.ToObjectReturnType.ItemsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getCode();

            @JsProperty
            GetPullDiagnosticResponse.ToObjectReturnType.ItemsListFieldType.CodeDescriptionFieldType getCodeDescription();

            @JsProperty
            GetPullDiagnosticResponse.ToObjectReturnType.ItemsListFieldType.GetDataUnionType getData();

            @JsProperty
            String getMessage();

            @JsProperty
            GetPullDiagnosticResponse.ToObjectReturnType.ItemsListFieldType.RangeFieldType getRange();

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
                    GetPullDiagnosticResponse.ToObjectReturnType.ItemsListFieldType.CodeDescriptionFieldType codeDescription);

            @JsProperty
            void setData(
                    GetPullDiagnosticResponse.ToObjectReturnType.ItemsListFieldType.GetDataUnionType data);

            @JsOverlay
            default void setData(String data) {
                setData(
                        Js.<GetPullDiagnosticResponse.ToObjectReturnType.ItemsListFieldType.GetDataUnionType>uncheckedCast(
                                data));
            }

            @JsOverlay
            default void setData(Uint8Array data) {
                setData(
                        Js.<GetPullDiagnosticResponse.ToObjectReturnType.ItemsListFieldType.GetDataUnionType>uncheckedCast(
                                data));
            }

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setRange(
                    GetPullDiagnosticResponse.ToObjectReturnType.ItemsListFieldType.RangeFieldType range);

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
        static GetPullDiagnosticResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<GetPullDiagnosticResponse.ToObjectReturnType.ItemsListFieldType> getItemsList();

        @JsProperty
        String getKind();

        @JsProperty
        String getResultId();

        @JsOverlay
        default void setItemsList(
                GetPullDiagnosticResponse.ToObjectReturnType.ItemsListFieldType[] itemsList) {
            setItemsList(
                    Js.<JsArray<GetPullDiagnosticResponse.ToObjectReturnType.ItemsListFieldType>>uncheckedCast(
                            itemsList));
        }

        @JsProperty
        void setItemsList(
                JsArray<GetPullDiagnosticResponse.ToObjectReturnType.ItemsListFieldType> itemsList);

        @JsProperty
        void setKind(String kind);

        @JsProperty
        void setResultId(String resultId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ItemsListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CodeDescriptionFieldType {
                @JsOverlay
                static GetPullDiagnosticResponse.ToObjectReturnType0.ItemsListFieldType.CodeDescriptionFieldType create() {
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
                static GetPullDiagnosticResponse.ToObjectReturnType0.ItemsListFieldType.GetDataUnionType of(
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
                    static GetPullDiagnosticResponse.ToObjectReturnType0.ItemsListFieldType.RangeFieldType.StartFieldType create() {
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
                static GetPullDiagnosticResponse.ToObjectReturnType0.ItemsListFieldType.RangeFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getEnd();

                @JsProperty
                GetPullDiagnosticResponse.ToObjectReturnType0.ItemsListFieldType.RangeFieldType.StartFieldType getStart();

                @JsProperty
                void setEnd(Object end);

                @JsProperty
                void setStart(
                        GetPullDiagnosticResponse.ToObjectReturnType0.ItemsListFieldType.RangeFieldType.StartFieldType start);
            }

            @JsOverlay
            static GetPullDiagnosticResponse.ToObjectReturnType0.ItemsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getCode();

            @JsProperty
            GetPullDiagnosticResponse.ToObjectReturnType0.ItemsListFieldType.CodeDescriptionFieldType getCodeDescription();

            @JsProperty
            GetPullDiagnosticResponse.ToObjectReturnType0.ItemsListFieldType.GetDataUnionType getData();

            @JsProperty
            String getMessage();

            @JsProperty
            GetPullDiagnosticResponse.ToObjectReturnType0.ItemsListFieldType.RangeFieldType getRange();

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
                    GetPullDiagnosticResponse.ToObjectReturnType0.ItemsListFieldType.CodeDescriptionFieldType codeDescription);

            @JsProperty
            void setData(
                    GetPullDiagnosticResponse.ToObjectReturnType0.ItemsListFieldType.GetDataUnionType data);

            @JsOverlay
            default void setData(String data) {
                setData(
                        Js.<GetPullDiagnosticResponse.ToObjectReturnType0.ItemsListFieldType.GetDataUnionType>uncheckedCast(
                                data));
            }

            @JsOverlay
            default void setData(Uint8Array data) {
                setData(
                        Js.<GetPullDiagnosticResponse.ToObjectReturnType0.ItemsListFieldType.GetDataUnionType>uncheckedCast(
                                data));
            }

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setRange(
                    GetPullDiagnosticResponse.ToObjectReturnType0.ItemsListFieldType.RangeFieldType range);

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
        static GetPullDiagnosticResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<GetPullDiagnosticResponse.ToObjectReturnType0.ItemsListFieldType> getItemsList();

        @JsProperty
        String getKind();

        @JsProperty
        String getResultId();

        @JsOverlay
        default void setItemsList(
                GetPullDiagnosticResponse.ToObjectReturnType0.ItemsListFieldType[] itemsList) {
            setItemsList(
                    Js.<JsArray<GetPullDiagnosticResponse.ToObjectReturnType0.ItemsListFieldType>>uncheckedCast(
                            itemsList));
        }

        @JsProperty
        void setItemsList(
                JsArray<GetPullDiagnosticResponse.ToObjectReturnType0.ItemsListFieldType> itemsList);

        @JsProperty
        void setKind(String kind);

        @JsProperty
        void setResultId(String resultId);
    }

    public static native GetPullDiagnosticResponse deserializeBinary(Uint8Array bytes);

    public static native GetPullDiagnosticResponse deserializeBinaryFromReader(
            GetPullDiagnosticResponse message, Object reader);

    public static native void serializeBinaryToWriter(
            GetPullDiagnosticResponse message, Object writer);

    public static native GetPullDiagnosticResponse.ToObjectReturnType toObject(
            boolean includeInstance, GetPullDiagnosticResponse msg);

    public native Diagnostic addItems();

    public native Diagnostic addItems(Diagnostic value, double index);

    public native Diagnostic addItems(Diagnostic value);

    public native void clearItemsList();

    public native void clearResultId();

    public native JsArray<Diagnostic> getItemsList();

    public native String getKind();

    public native String getResultId();

    public native boolean hasResultId();

    public native Uint8Array serializeBinary();

    @JsOverlay
    public final void setItemsList(Diagnostic[] value) {
        setItemsList(Js.<JsArray<Diagnostic>>uncheckedCast(value));
    }

    public native void setItemsList(JsArray<Diagnostic> value);

    public native void setKind(String value);

    public native void setResultId(String value);

    public native GetPullDiagnosticResponse.ToObjectReturnType0 toObject();

    public native GetPullDiagnosticResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
