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
        name = "dhinternal.io.deephaven_core.proto.console_pb.GetDiagnosticRequest",
        namespace = JsPackage.GLOBAL)
public class GetDiagnosticRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TextDocumentFieldType {
            @JsOverlay
            static GetDiagnosticRequest.ToObjectReturnType.TextDocumentFieldType create() {
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
        static GetDiagnosticRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getIdentifier();

        @JsProperty
        String getPreviousResultId();

        @JsProperty
        GetDiagnosticRequest.ToObjectReturnType.TextDocumentFieldType getTextDocument();

        @JsProperty
        void setIdentifier(String identifier);

        @JsProperty
        void setPreviousResultId(String previousResultId);

        @JsProperty
        void setTextDocument(
                GetDiagnosticRequest.ToObjectReturnType.TextDocumentFieldType textDocument);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TextDocumentFieldType {
            @JsOverlay
            static GetDiagnosticRequest.ToObjectReturnType0.TextDocumentFieldType create() {
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
        static GetDiagnosticRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getIdentifier();

        @JsProperty
        String getPreviousResultId();

        @JsProperty
        GetDiagnosticRequest.ToObjectReturnType0.TextDocumentFieldType getTextDocument();

        @JsProperty
        void setIdentifier(String identifier);

        @JsProperty
        void setPreviousResultId(String previousResultId);

        @JsProperty
        void setTextDocument(
                GetDiagnosticRequest.ToObjectReturnType0.TextDocumentFieldType textDocument);
    }

    public static native GetDiagnosticRequest deserializeBinary(Uint8Array bytes);

    public static native GetDiagnosticRequest deserializeBinaryFromReader(
            GetDiagnosticRequest message, Object reader);

    public static native void serializeBinaryToWriter(GetDiagnosticRequest message, Object writer);

    public static native GetDiagnosticRequest.ToObjectReturnType toObject(
            boolean includeInstance, GetDiagnosticRequest msg);

    public native void clearIdentifier();

    public native void clearPreviousResultId();

    public native void clearTextDocument();

    public native String getIdentifier();

    public native String getPreviousResultId();

    public native VersionedTextDocumentIdentifier getTextDocument();

    public native boolean hasIdentifier();

    public native boolean hasPreviousResultId();

    public native boolean hasTextDocument();

    public native Uint8Array serializeBinary();

    public native void setIdentifier(String value);

    public native void setPreviousResultId(String value);

    public native void setTextDocument();

    public native void setTextDocument(VersionedTextDocumentIdentifier value);

    public native GetDiagnosticRequest.ToObjectReturnType0 toObject();

    public native GetDiagnosticRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
