package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.changedocumentrequest;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.DocumentRange;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.ChangeDocumentRequest.TextDocumentContentChangeEvent",
    namespace = JsPackage.GLOBAL)
public class TextDocumentContentChangeEvent {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface RangeFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface StartFieldType {
                @JsOverlay
                static TextDocumentContentChangeEvent.ToObjectReturnType.RangeFieldType.StartFieldType create() {
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
            static TextDocumentContentChangeEvent.ToObjectReturnType.RangeFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getEnd();

            @JsProperty
            TextDocumentContentChangeEvent.ToObjectReturnType.RangeFieldType.StartFieldType getStart();

            @JsProperty
            void setEnd(Object end);

            @JsProperty
            void setStart(
                TextDocumentContentChangeEvent.ToObjectReturnType.RangeFieldType.StartFieldType start);
        }

        @JsOverlay
        static TextDocumentContentChangeEvent.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        TextDocumentContentChangeEvent.ToObjectReturnType.RangeFieldType getRange();

        @JsProperty
        double getRangeLength();

        @JsProperty
        String getText();

        @JsProperty
        void setRange(TextDocumentContentChangeEvent.ToObjectReturnType.RangeFieldType range);

        @JsProperty
        void setRangeLength(double rangeLength);

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
                static TextDocumentContentChangeEvent.ToObjectReturnType0.RangeFieldType.StartFieldType create() {
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
            static TextDocumentContentChangeEvent.ToObjectReturnType0.RangeFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getEnd();

            @JsProperty
            TextDocumentContentChangeEvent.ToObjectReturnType0.RangeFieldType.StartFieldType getStart();

            @JsProperty
            void setEnd(Object end);

            @JsProperty
            void setStart(
                TextDocumentContentChangeEvent.ToObjectReturnType0.RangeFieldType.StartFieldType start);
        }

        @JsOverlay
        static TextDocumentContentChangeEvent.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        TextDocumentContentChangeEvent.ToObjectReturnType0.RangeFieldType getRange();

        @JsProperty
        double getRangeLength();

        @JsProperty
        String getText();

        @JsProperty
        void setRange(TextDocumentContentChangeEvent.ToObjectReturnType0.RangeFieldType range);

        @JsProperty
        void setRangeLength(double rangeLength);

        @JsProperty
        void setText(String text);
    }

    public static native TextDocumentContentChangeEvent deserializeBinary(Uint8Array bytes);

    public static native TextDocumentContentChangeEvent deserializeBinaryFromReader(
        TextDocumentContentChangeEvent message, Object reader);

    public static native void serializeBinaryToWriter(
        TextDocumentContentChangeEvent message, Object writer);

    public static native TextDocumentContentChangeEvent.ToObjectReturnType toObject(
        boolean includeInstance, TextDocumentContentChangeEvent msg);

    public native void clearRange();

    public native DocumentRange getRange();

    public native double getRangeLength();

    public native String getText();

    public native boolean hasRange();

    public native Uint8Array serializeBinary();

    public native void setRange();

    public native void setRange(DocumentRange value);

    public native void setRangeLength(double value);

    public native void setText(String value);

    public native TextDocumentContentChangeEvent.ToObjectReturnType0 toObject();

    public native TextDocumentContentChangeEvent.ToObjectReturnType0 toObject(
        boolean includeInstance);
}
