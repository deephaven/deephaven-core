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
        name = "dhinternal.io.deephaven_core.proto.console_pb.GetHoverRequest",
        namespace = JsPackage.GLOBAL)
public class GetHoverRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface PositionFieldType {
            @JsOverlay
            static GetHoverRequest.ToObjectReturnType.PositionFieldType create() {
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

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TextDocumentFieldType {
            @JsOverlay
            static GetHoverRequest.ToObjectReturnType.TextDocumentFieldType create() {
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

        @JsOverlay
        static GetHoverRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        GetHoverRequest.ToObjectReturnType.PositionFieldType getPosition();

        @JsProperty
        GetHoverRequest.ToObjectReturnType.TextDocumentFieldType getTextDocument();

        @JsProperty
        void setPosition(GetHoverRequest.ToObjectReturnType.PositionFieldType position);

        @JsProperty
        void setTextDocument(GetHoverRequest.ToObjectReturnType.TextDocumentFieldType textDocument);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface PositionFieldType {
            @JsOverlay
            static GetHoverRequest.ToObjectReturnType0.PositionFieldType create() {
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

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TextDocumentFieldType {
            @JsOverlay
            static GetHoverRequest.ToObjectReturnType0.TextDocumentFieldType create() {
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

        @JsOverlay
        static GetHoverRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        GetHoverRequest.ToObjectReturnType0.PositionFieldType getPosition();

        @JsProperty
        GetHoverRequest.ToObjectReturnType0.TextDocumentFieldType getTextDocument();

        @JsProperty
        void setPosition(GetHoverRequest.ToObjectReturnType0.PositionFieldType position);

        @JsProperty
        void setTextDocument(GetHoverRequest.ToObjectReturnType0.TextDocumentFieldType textDocument);
    }

    public static native GetHoverRequest deserializeBinary(Uint8Array bytes);

    public static native GetHoverRequest deserializeBinaryFromReader(
            GetHoverRequest message, Object reader);

    public static native void serializeBinaryToWriter(GetHoverRequest message, Object writer);

    public static native GetHoverRequest.ToObjectReturnType toObject(
            boolean includeInstance, GetHoverRequest msg);

    public native void clearPosition();

    public native void clearTextDocument();

    public native Position getPosition();

    public native VersionedTextDocumentIdentifier getTextDocument();

    public native boolean hasPosition();

    public native boolean hasTextDocument();

    public native Uint8Array serializeBinary();

    public native void setPosition();

    public native void setPosition(Position value);

    public native void setTextDocument();

    public native void setTextDocument(VersionedTextDocumentIdentifier value);

    public native GetHoverRequest.ToObjectReturnType0 toObject();

    public native GetHoverRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
