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
        name = "dhinternal.io.deephaven.proto.console_pb.TextEdit",
        namespace = JsPackage.GLOBAL)
public class TextEdit {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface RangeFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface StartFieldType {
                @JsOverlay
                static TextEdit.ToObjectReturnType.RangeFieldType.StartFieldType create() {
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
            static TextEdit.ToObjectReturnType.RangeFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getEnd();

            @JsProperty
            TextEdit.ToObjectReturnType.RangeFieldType.StartFieldType getStart();

            @JsProperty
            void setEnd(Object end);

            @JsProperty
            void setStart(TextEdit.ToObjectReturnType.RangeFieldType.StartFieldType start);
        }

        @JsOverlay
        static TextEdit.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        TextEdit.ToObjectReturnType.RangeFieldType getRange();

        @JsProperty
        String getText();

        @JsProperty
        void setRange(TextEdit.ToObjectReturnType.RangeFieldType range);

        @JsProperty
        void setText(String text);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface RangeFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface StartFieldType {
                @JsOverlay
                static TextEdit.ToObjectReturnType0.RangeFieldType.StartFieldType create() {
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
            static TextEdit.ToObjectReturnType0.RangeFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getEnd();

            @JsProperty
            TextEdit.ToObjectReturnType0.RangeFieldType.StartFieldType getStart();

            @JsProperty
            void setEnd(Object end);

            @JsProperty
            void setStart(TextEdit.ToObjectReturnType0.RangeFieldType.StartFieldType start);
        }

        @JsOverlay
        static TextEdit.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        TextEdit.ToObjectReturnType0.RangeFieldType getRange();

        @JsProperty
        String getText();

        @JsProperty
        void setRange(TextEdit.ToObjectReturnType0.RangeFieldType range);

        @JsProperty
        void setText(String text);
    }

    public static native TextEdit deserializeBinary(Uint8Array bytes);

    public static native TextEdit deserializeBinaryFromReader(TextEdit message, Object reader);

    public static native void serializeBinaryToWriter(TextEdit message, Object writer);

    public static native TextEdit.ToObjectReturnType toObject(boolean includeInstance, TextEdit msg);

    public native void clearRange();

    public native DocumentRange getRange();

    public native String getText();

    public native boolean hasRange();

    public native Uint8Array serializeBinary();

    public native void setRange();

    public native void setRange(DocumentRange value);

    public native void setText(String value);

    public native TextEdit.ToObjectReturnType0 toObject();

    public native TextEdit.ToObjectReturnType0 toObject(boolean includeInstance);
}
