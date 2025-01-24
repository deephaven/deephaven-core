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
        name = "dhinternal.io.deephaven_core.proto.console_pb.GetHoverResponse",
        namespace = JsPackage.GLOBAL)
public class GetHoverResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ContentsFieldType {
            @JsOverlay
            static GetHoverResponse.ToObjectReturnType.ContentsFieldType create() {
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
        public interface RangeFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface StartFieldType {
                @JsOverlay
                static GetHoverResponse.ToObjectReturnType.RangeFieldType.StartFieldType create() {
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
            static GetHoverResponse.ToObjectReturnType.RangeFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getEnd();

            @JsProperty
            GetHoverResponse.ToObjectReturnType.RangeFieldType.StartFieldType getStart();

            @JsProperty
            void setEnd(Object end);

            @JsProperty
            void setStart(GetHoverResponse.ToObjectReturnType.RangeFieldType.StartFieldType start);
        }

        @JsOverlay
        static GetHoverResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        GetHoverResponse.ToObjectReturnType.ContentsFieldType getContents();

        @JsProperty
        GetHoverResponse.ToObjectReturnType.RangeFieldType getRange();

        @JsProperty
        void setContents(GetHoverResponse.ToObjectReturnType.ContentsFieldType contents);

        @JsProperty
        void setRange(GetHoverResponse.ToObjectReturnType.RangeFieldType range);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ContentsFieldType {
            @JsOverlay
            static GetHoverResponse.ToObjectReturnType0.ContentsFieldType create() {
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
        public interface RangeFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface StartFieldType {
                @JsOverlay
                static GetHoverResponse.ToObjectReturnType0.RangeFieldType.StartFieldType create() {
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
            static GetHoverResponse.ToObjectReturnType0.RangeFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getEnd();

            @JsProperty
            GetHoverResponse.ToObjectReturnType0.RangeFieldType.StartFieldType getStart();

            @JsProperty
            void setEnd(Object end);

            @JsProperty
            void setStart(GetHoverResponse.ToObjectReturnType0.RangeFieldType.StartFieldType start);
        }

        @JsOverlay
        static GetHoverResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        GetHoverResponse.ToObjectReturnType0.ContentsFieldType getContents();

        @JsProperty
        GetHoverResponse.ToObjectReturnType0.RangeFieldType getRange();

        @JsProperty
        void setContents(GetHoverResponse.ToObjectReturnType0.ContentsFieldType contents);

        @JsProperty
        void setRange(GetHoverResponse.ToObjectReturnType0.RangeFieldType range);
    }

    public static native GetHoverResponse deserializeBinary(Uint8Array bytes);

    public static native GetHoverResponse deserializeBinaryFromReader(
            GetHoverResponse message, Object reader);

    public static native void serializeBinaryToWriter(GetHoverResponse message, Object writer);

    public static native GetHoverResponse.ToObjectReturnType toObject(
            boolean includeInstance, GetHoverResponse msg);

    public native void clearContents();

    public native void clearRange();

    public native MarkupContent getContents();

    public native DocumentRange getRange();

    public native boolean hasContents();

    public native boolean hasRange();

    public native Uint8Array serializeBinary();

    public native void setContents();

    public native void setContents(MarkupContent value);

    public native void setRange();

    public native void setRange(DocumentRange value);

    public native GetHoverResponse.ToObjectReturnType0 toObject();

    public native GetHoverResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
