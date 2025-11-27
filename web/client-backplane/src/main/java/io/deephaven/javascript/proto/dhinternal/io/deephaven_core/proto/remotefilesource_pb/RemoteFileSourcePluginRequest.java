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
        name = "dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourcePluginRequest",
        namespace = JsPackage.GLOBAL)
public class RemoteFileSourcePluginRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SetConnectionIdFieldType {
            @JsOverlay
            static RemoteFileSourcePluginRequest.ToObjectReturnType.SetConnectionIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getConnectionId();

            @JsProperty
            void setConnectionId(String connectionId);
        }

        @JsOverlay
        static RemoteFileSourcePluginRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getMeta();

        @JsProperty
        String getRequestId();

        @JsProperty
        RemoteFileSourcePluginRequest.ToObjectReturnType.SetConnectionIdFieldType getSetConnectionId();

        @JsProperty
        void setMeta(Object meta);

        @JsProperty
        void setRequestId(String requestId);

        @JsProperty
        void setSetConnectionId(
                RemoteFileSourcePluginRequest.ToObjectReturnType.SetConnectionIdFieldType setConnectionId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SetConnectionIdFieldType {
            @JsOverlay
            static RemoteFileSourcePluginRequest.ToObjectReturnType0.SetConnectionIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getConnectionId();

            @JsProperty
            void setConnectionId(String connectionId);
        }

        @JsOverlay
        static RemoteFileSourcePluginRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getMeta();

        @JsProperty
        String getRequestId();

        @JsProperty
        RemoteFileSourcePluginRequest.ToObjectReturnType0.SetConnectionIdFieldType getSetConnectionId();

        @JsProperty
        void setMeta(Object meta);

        @JsProperty
        void setRequestId(String requestId);

        @JsProperty
        void setSetConnectionId(
                RemoteFileSourcePluginRequest.ToObjectReturnType0.SetConnectionIdFieldType setConnectionId);
    }

    public static native RemoteFileSourcePluginRequest deserializeBinary(Uint8Array bytes);

    public static native RemoteFileSourcePluginRequest deserializeBinaryFromReader(
            RemoteFileSourcePluginRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            RemoteFileSourcePluginRequest message, Object writer);

    public static native RemoteFileSourcePluginRequest.ToObjectReturnType toObject(
            boolean includeInstance, RemoteFileSourcePluginRequest msg);

    public native void clearMeta();

    public native void clearSetConnectionId();

    public native RemoteFileSourceMetaRequest getMeta();

    public native int getRequestCase();

    public native String getRequestId();

    public native RemoteFileSourceSetConnectionIdRequest getSetConnectionId();

    public native boolean hasMeta();

    public native boolean hasSetConnectionId();

    public native Uint8Array serializeBinary();

    public native void setMeta();

    public native void setMeta(RemoteFileSourceMetaRequest value);

    public native void setRequestId(String value);

    public native void setSetConnectionId();

    public native void setSetConnectionId(RemoteFileSourceSetConnectionIdRequest value);

    public native RemoteFileSourcePluginRequest.ToObjectReturnType0 toObject();

    public native RemoteFileSourcePluginRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
