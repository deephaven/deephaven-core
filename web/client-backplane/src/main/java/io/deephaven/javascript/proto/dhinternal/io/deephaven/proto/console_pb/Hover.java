/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.console_pb.Hover",
        namespace = JsPackage.GLOBAL)
public class Hover {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface RangeFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface StartFieldType {
                @JsOverlay
                static Hover.ToObjectReturnType.RangeFieldType.StartFieldType create() {
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
            static Hover.ToObjectReturnType.RangeFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getEnd();

            @JsProperty
            Hover.ToObjectReturnType.RangeFieldType.StartFieldType getStart();

            @JsProperty
            void setEnd(Object end);

            @JsProperty
            void setStart(Hover.ToObjectReturnType.RangeFieldType.StartFieldType start);
        }

        @JsOverlay
        static Hover.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getContents();

        @JsProperty
        Hover.ToObjectReturnType.RangeFieldType getRange();

        @JsProperty
        void setContents(String contents);

        @JsProperty
        void setRange(Hover.ToObjectReturnType.RangeFieldType range);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface RangeFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface StartFieldType {
                @JsOverlay
                static Hover.ToObjectReturnType0.RangeFieldType.StartFieldType create() {
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
            static Hover.ToObjectReturnType0.RangeFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getEnd();

            @JsProperty
            Hover.ToObjectReturnType0.RangeFieldType.StartFieldType getStart();

            @JsProperty
            void setEnd(Object end);

            @JsProperty
            void setStart(Hover.ToObjectReturnType0.RangeFieldType.StartFieldType start);
        }

        @JsOverlay
        static Hover.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getContents();

        @JsProperty
        Hover.ToObjectReturnType0.RangeFieldType getRange();

        @JsProperty
        void setContents(String contents);

        @JsProperty
        void setRange(Hover.ToObjectReturnType0.RangeFieldType range);
    }

    public static native Hover deserializeBinary(Uint8Array bytes);

    public static native Hover deserializeBinaryFromReader(Hover message, Object reader);

    public static native void serializeBinaryToWriter(Hover message, Object writer);

    public static native Hover.ToObjectReturnType toObject(boolean includeInstance, Hover msg);

    public native void clearRange();

    public native String getContents();

    public native DocumentRange getRange();

    public native boolean hasRange();

    public native Uint8Array serializeBinary();

    public native void setContents(String value);

    public native void setRange();

    public native void setRange(DocumentRange value);

    public native Hover.ToObjectReturnType0 toObject();

    public native Hover.ToObjectReturnType0 toObject(boolean includeInstance);
}
