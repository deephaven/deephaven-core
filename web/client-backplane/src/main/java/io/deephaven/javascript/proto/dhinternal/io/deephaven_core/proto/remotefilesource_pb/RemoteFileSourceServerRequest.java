//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
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
        name = "dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceServerRequest",
        namespace = JsPackage.GLOBAL)
public class RemoteFileSourceServerRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface MetaRequestFieldType {
            @JsOverlay
            static RemoteFileSourceServerRequest.ToObjectReturnType.MetaRequestFieldType create() {
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
            static RemoteFileSourceServerRequest.ToObjectReturnType.SetExecutionContextResponseFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            boolean isSuccess();

            @JsProperty
            void setSuccess(boolean success);
        }

        @JsOverlay
        static RemoteFileSourceServerRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        RemoteFileSourceServerRequest.ToObjectReturnType.MetaRequestFieldType getMetaRequest();

        @JsProperty
        String getRequestId();

        @JsProperty
        RemoteFileSourceServerRequest.ToObjectReturnType.SetExecutionContextResponseFieldType getSetExecutionContextResponse();

        @JsProperty
        void setMetaRequest(
                RemoteFileSourceServerRequest.ToObjectReturnType.MetaRequestFieldType metaRequest);

        @JsProperty
        void setRequestId(String requestId);

        @JsProperty
        void setSetExecutionContextResponse(
                RemoteFileSourceServerRequest.ToObjectReturnType.SetExecutionContextResponseFieldType setExecutionContextResponse);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface MetaRequestFieldType {
            @JsOverlay
            static RemoteFileSourceServerRequest.ToObjectReturnType0.MetaRequestFieldType create() {
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
            static RemoteFileSourceServerRequest.ToObjectReturnType0.SetExecutionContextResponseFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            boolean isSuccess();

            @JsProperty
            void setSuccess(boolean success);
        }

        @JsOverlay
        static RemoteFileSourceServerRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        RemoteFileSourceServerRequest.ToObjectReturnType0.MetaRequestFieldType getMetaRequest();

        @JsProperty
        String getRequestId();

        @JsProperty
        RemoteFileSourceServerRequest.ToObjectReturnType0.SetExecutionContextResponseFieldType getSetExecutionContextResponse();

        @JsProperty
        void setMetaRequest(
                RemoteFileSourceServerRequest.ToObjectReturnType0.MetaRequestFieldType metaRequest);

        @JsProperty
        void setRequestId(String requestId);

        @JsProperty
        void setSetExecutionContextResponse(
                RemoteFileSourceServerRequest.ToObjectReturnType0.SetExecutionContextResponseFieldType setExecutionContextResponse);
    }

    public static native RemoteFileSourceServerRequest deserializeBinary(Uint8Array bytes);

    public static native RemoteFileSourceServerRequest deserializeBinaryFromReader(
            RemoteFileSourceServerRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            RemoteFileSourceServerRequest message, Object writer);

    public static native RemoteFileSourceServerRequest.ToObjectReturnType toObject(
            boolean includeInstance, RemoteFileSourceServerRequest msg);

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

    public native RemoteFileSourceServerRequest.ToObjectReturnType0 toObject();

    public native RemoteFileSourceServerRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
