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
        name = "dhinternal.io.deephaven.proto.console_pb.TextDocumentItem",
        namespace = JsPackage.GLOBAL)
public class TextDocumentItem {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static TextDocumentItem.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getLanguageId();

        @JsProperty
        String getText();

        @JsProperty
        String getUri();

        @JsProperty
        double getVersion();

        @JsProperty
        void setLanguageId(String languageId);

        @JsProperty
        void setText(String text);

        @JsProperty
        void setUri(String uri);

        @JsProperty
        void setVersion(double version);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static TextDocumentItem.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getLanguageId();

        @JsProperty
        String getText();

        @JsProperty
        String getUri();

        @JsProperty
        double getVersion();

        @JsProperty
        void setLanguageId(String languageId);

        @JsProperty
        void setText(String text);

        @JsProperty
        void setUri(String uri);

        @JsProperty
        void setVersion(double version);
    }

    public static native TextDocumentItem deserializeBinary(Uint8Array bytes);

    public static native TextDocumentItem deserializeBinaryFromReader(
            TextDocumentItem message, Object reader);

    public static native void serializeBinaryToWriter(TextDocumentItem message, Object writer);

    public static native TextDocumentItem.ToObjectReturnType toObject(
            boolean includeInstance, TextDocumentItem msg);

    public native String getLanguageId();

    public native String getText();

    public native String getUri();

    public native double getVersion();

    public native Uint8Array serializeBinary();

    public native void setLanguageId(String value);

    public native void setText(String value);

    public native void setUri(String value);

    public native void setVersion(double value);

    public native TextDocumentItem.ToObjectReturnType0 toObject();

    public native TextDocumentItem.ToObjectReturnType0 toObject(boolean includeInstance);
}
