//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.diagnostic.CodeDescription;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.diagnostic.DiagnosticSeverityMap;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.diagnostic.DiagnosticTagMap;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.console_pb.Diagnostic",
        namespace = JsPackage.GLOBAL)
public class Diagnostic {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetDataUnionType {
        @JsOverlay
        static Diagnostic.GetDataUnionType of(Object o) {
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
    public interface SetDataValueUnionType {
        @JsOverlay
        static Diagnostic.SetDataValueUnionType of(Object o) {
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
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface CodeDescriptionFieldType {
            @JsOverlay
            static Diagnostic.ToObjectReturnType.CodeDescriptionFieldType create() {
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
            static Diagnostic.ToObjectReturnType.GetDataUnionType of(Object o) {
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
                static Diagnostic.ToObjectReturnType.RangeFieldType.StartFieldType create() {
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
            static Diagnostic.ToObjectReturnType.RangeFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getEnd();

            @JsProperty
            Diagnostic.ToObjectReturnType.RangeFieldType.StartFieldType getStart();

            @JsProperty
            void setEnd(Object end);

            @JsProperty
            void setStart(Diagnostic.ToObjectReturnType.RangeFieldType.StartFieldType start);
        }

        @JsOverlay
        static Diagnostic.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getCode();

        @JsProperty
        Diagnostic.ToObjectReturnType.CodeDescriptionFieldType getCodeDescription();

        @JsProperty
        Diagnostic.ToObjectReturnType.GetDataUnionType getData();

        @JsProperty
        String getMessage();

        @JsProperty
        Diagnostic.ToObjectReturnType.RangeFieldType getRange();

        @JsProperty
        double getSeverity();

        @JsProperty
        String getSource();

        @JsProperty
        JsArray<Double> getTagsList();

        @JsProperty
        void setCode(String code);

        @JsProperty
        void setCodeDescription(Diagnostic.ToObjectReturnType.CodeDescriptionFieldType codeDescription);

        @JsProperty
        void setData(Diagnostic.ToObjectReturnType.GetDataUnionType data);

        @JsOverlay
        default void setData(String data) {
            setData(Js.<Diagnostic.ToObjectReturnType.GetDataUnionType>uncheckedCast(data));
        }

        @JsOverlay
        default void setData(Uint8Array data) {
            setData(Js.<Diagnostic.ToObjectReturnType.GetDataUnionType>uncheckedCast(data));
        }

        @JsProperty
        void setMessage(String message);

        @JsProperty
        void setRange(Diagnostic.ToObjectReturnType.RangeFieldType range);

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

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface CodeDescriptionFieldType {
            @JsOverlay
            static Diagnostic.ToObjectReturnType0.CodeDescriptionFieldType create() {
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
            static Diagnostic.ToObjectReturnType0.GetDataUnionType of(Object o) {
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
                static Diagnostic.ToObjectReturnType0.RangeFieldType.StartFieldType create() {
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
            static Diagnostic.ToObjectReturnType0.RangeFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getEnd();

            @JsProperty
            Diagnostic.ToObjectReturnType0.RangeFieldType.StartFieldType getStart();

            @JsProperty
            void setEnd(Object end);

            @JsProperty
            void setStart(Diagnostic.ToObjectReturnType0.RangeFieldType.StartFieldType start);
        }

        @JsOverlay
        static Diagnostic.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getCode();

        @JsProperty
        Diagnostic.ToObjectReturnType0.CodeDescriptionFieldType getCodeDescription();

        @JsProperty
        Diagnostic.ToObjectReturnType0.GetDataUnionType getData();

        @JsProperty
        String getMessage();

        @JsProperty
        Diagnostic.ToObjectReturnType0.RangeFieldType getRange();

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
                Diagnostic.ToObjectReturnType0.CodeDescriptionFieldType codeDescription);

        @JsProperty
        void setData(Diagnostic.ToObjectReturnType0.GetDataUnionType data);

        @JsOverlay
        default void setData(String data) {
            setData(Js.<Diagnostic.ToObjectReturnType0.GetDataUnionType>uncheckedCast(data));
        }

        @JsOverlay
        default void setData(Uint8Array data) {
            setData(Js.<Diagnostic.ToObjectReturnType0.GetDataUnionType>uncheckedCast(data));
        }

        @JsProperty
        void setMessage(String message);

        @JsProperty
        void setRange(Diagnostic.ToObjectReturnType0.RangeFieldType range);

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

    public static DiagnosticSeverityMap DiagnosticSeverity;
    public static DiagnosticTagMap DiagnosticTag;

    public static native Diagnostic deserializeBinary(Uint8Array bytes);

    public static native Diagnostic deserializeBinaryFromReader(Diagnostic message, Object reader);

    public static native void serializeBinaryToWriter(Diagnostic message, Object writer);

    public static native Diagnostic.ToObjectReturnType toObject(
            boolean includeInstance, Diagnostic msg);

    public native double addTags(double value, double index);

    public native double addTags(double value);

    public native void clearCode();

    public native void clearCodeDescription();

    public native void clearData();

    public native void clearRange();

    public native void clearSource();

    public native void clearTagsList();

    public native String getCode();

    public native CodeDescription getCodeDescription();

    public native Diagnostic.GetDataUnionType getData();

    public native String getData_asB64();

    public native Uint8Array getData_asU8();

    public native String getMessage();

    public native DocumentRange getRange();

    public native double getSeverity();

    public native String getSource();

    public native JsArray<Double> getTagsList();

    public native boolean hasCode();

    public native boolean hasCodeDescription();

    public native boolean hasData();

    public native boolean hasRange();

    public native boolean hasSource();

    public native Uint8Array serializeBinary();

    public native void setCode(String value);

    public native void setCodeDescription();

    public native void setCodeDescription(CodeDescription value);

    public native void setData(Diagnostic.SetDataValueUnionType value);

    @JsOverlay
    public final void setData(String value) {
        setData(Js.<Diagnostic.SetDataValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setData(Uint8Array value) {
        setData(Js.<Diagnostic.SetDataValueUnionType>uncheckedCast(value));
    }

    public native void setMessage(String value);

    public native void setRange();

    public native void setRange(DocumentRange value);

    public native void setSeverity(double value);

    public native void setSource(String value);

    public native void setTagsList(JsArray<Double> value);

    @JsOverlay
    public final void setTagsList(double[] value) {
        setTagsList(Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native Diagnostic.ToObjectReturnType0 toObject();

    public native Diagnostic.ToObjectReturnType0 toObject(boolean includeInstance);
}
