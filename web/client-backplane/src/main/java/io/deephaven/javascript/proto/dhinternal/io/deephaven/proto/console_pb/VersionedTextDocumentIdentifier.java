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
        name = "dhinternal.io.deephaven.proto.console_pb.VersionedTextDocumentIdentifier",
        namespace = JsPackage.GLOBAL)
public class VersionedTextDocumentIdentifier {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static VersionedTextDocumentIdentifier.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getUri();

        @JsProperty
        double getVersion();

        @JsProperty
        void setUri(String uri);

        @JsProperty
        void setVersion(double version);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static VersionedTextDocumentIdentifier.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getUri();

        @JsProperty
        double getVersion();

        @JsProperty
        void setUri(String uri);

        @JsProperty
        void setVersion(double version);
    }

    public static native VersionedTextDocumentIdentifier deserializeBinary(Uint8Array bytes);

    public static native VersionedTextDocumentIdentifier deserializeBinaryFromReader(
            VersionedTextDocumentIdentifier message, Object reader);

    public static native void serializeBinaryToWriter(
            VersionedTextDocumentIdentifier message, Object writer);

    public static native VersionedTextDocumentIdentifier.ToObjectReturnType toObject(
            boolean includeInstance, VersionedTextDocumentIdentifier msg);

    public native String getUri();

    public native double getVersion();

    public native Uint8Array serializeBinary();

    public native void setUri(String value);

    public native void setVersion(double value);

    public native VersionedTextDocumentIdentifier.ToObjectReturnType0 toObject();

    public native VersionedTextDocumentIdentifier.ToObjectReturnType0 toObject(
            boolean includeInstance);
}
