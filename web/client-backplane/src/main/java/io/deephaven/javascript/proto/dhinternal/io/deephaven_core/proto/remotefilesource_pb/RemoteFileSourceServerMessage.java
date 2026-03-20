//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceServerMessage",
        namespace = JsPackage.GLOBAL)
public class RemoteFileSourceServerMessage {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface MetaRequestFieldType {
            @JsOverlay
            static RemoteFileSourceServerMessage.ToObjectReturnType.MetaRequestFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getResourceName();

            @JsProperty
            void setResourceName(String resourceName);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SetExecutionContextResponseFieldType {
            @JsOverlay
            static RemoteFileSourceServerMessage.ToObjectReturnType.SetExecutionContextResponseFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            boolean isSuccess();

            @JsProperty
            void setSuccess(boolean success);
        }

        @JsOverlay
        static RemoteFileSourceServerMessage.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        RemoteFileSourceServerMessage.ToObjectReturnType.MetaRequestFieldType getMetaRequest();

        @JsProperty
        String getRequestId();

        @JsProperty
        RemoteFileSourceServerMessage.ToObjectReturnType.SetExecutionContextResponseFieldType getSetExecutionContextResponse();

        @JsProperty
        void setMetaRequest(
                RemoteFileSourceServerMessage.ToObjectReturnType.MetaRequestFieldType metaRequest);

        @JsProperty
        void setRequestId(String requestId);

        @JsProperty
        void setSetExecutionContextResponse(
                RemoteFileSourceServerMessage.ToObjectReturnType.SetExecutionContextResponseFieldType setExecutionContextResponse);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface MetaRequestFieldType {
            @JsOverlay
            static RemoteFileSourceServerMessage.ToObjectReturnType0.MetaRequestFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getResourceName();

            @JsProperty
            void setResourceName(String resourceName);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SetExecutionContextResponseFieldType {
            @JsOverlay
            static RemoteFileSourceServerMessage.ToObjectReturnType0.SetExecutionContextResponseFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            boolean isSuccess();

            @JsProperty
            void setSuccess(boolean success);
        }

        @JsOverlay
        static RemoteFileSourceServerMessage.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        RemoteFileSourceServerMessage.ToObjectReturnType0.MetaRequestFieldType getMetaRequest();

        @JsProperty
        String getRequestId();

        @JsProperty
        RemoteFileSourceServerMessage.ToObjectReturnType0.SetExecutionContextResponseFieldType getSetExecutionContextResponse();

        @JsProperty
        void setMetaRequest(
                RemoteFileSourceServerMessage.ToObjectReturnType0.MetaRequestFieldType metaRequest);

        @JsProperty
        void setRequestId(String requestId);

        @JsProperty
        void setSetExecutionContextResponse(
                RemoteFileSourceServerMessage.ToObjectReturnType0.SetExecutionContextResponseFieldType setExecutionContextResponse);
    }

    public static native RemoteFileSourceServerMessage deserializeBinary(Uint8Array bytes);

    public static native RemoteFileSourceServerMessage deserializeBinaryFromReader(
            RemoteFileSourceServerMessage message, Object reader);

    public static native void serializeBinaryToWriter(
            RemoteFileSourceServerMessage message, Object writer);

    public static native RemoteFileSourceServerMessage.ToObjectReturnType toObject(
            boolean includeInstance, RemoteFileSourceServerMessage msg);

    public native void clearMetaRequest();

    public native void clearSetExecutionContextResponse();

    public native RemoteFileSourceMetaRequest getMetaRequest();

    public native int getRequestCase();

    public native String getRequestId();

    public native SetExecutionContextResponse getSetExecutionContextResponse();

    public native boolean hasMetaRequest();

    public native boolean hasSetExecutionContextResponse();

    public native Uint8Array serializeBinary();

    public native void setMetaRequest();

    public native void setMetaRequest(RemoteFileSourceMetaRequest value);

    public native void setRequestId(String value);

    public native void setSetExecutionContextResponse();

    public native void setSetExecutionContextResponse(SetExecutionContextResponse value);

    public native RemoteFileSourceServerMessage.ToObjectReturnType0 toObject();

    public native RemoteFileSourceServerMessage.ToObjectReturnType0 toObject(boolean includeInstance);
}
