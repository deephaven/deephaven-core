//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceClientMessage",
        namespace = JsPackage.GLOBAL)
public class RemoteFileSourceClientMessage {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface MetaResponseFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetContentUnionType {
                @JsOverlay
                static RemoteFileSourceClientMessage.ToObjectReturnType.MetaResponseFieldType.GetContentUnionType of(
                        Object o) {
                    return Js.cast(o);
                }

                @JsOverlay
                default String asString() {
                    return Js.asString(this);
                }

                @JsOverlay
                default Uint8Array asUint8Array() {
                    return Js.cast(this);
                }

                @JsOverlay
                default boolean isString() {
                    return (Object) this instanceof String;
                }

                @JsOverlay
                default boolean isUint8Array() {
                    return (Object) this instanceof Uint8Array;
                }
            }

            @JsOverlay
            static RemoteFileSourceClientMessage.ToObjectReturnType.MetaResponseFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            RemoteFileSourceClientMessage.ToObjectReturnType.MetaResponseFieldType.GetContentUnionType getContent();

            @JsProperty
            String getError();

            @JsProperty
            boolean isFound();

            @JsProperty
            void setContent(
                    RemoteFileSourceClientMessage.ToObjectReturnType.MetaResponseFieldType.GetContentUnionType content);

            @JsOverlay
            default void setContent(String content) {
                setContent(
                        Js.<RemoteFileSourceClientMessage.ToObjectReturnType.MetaResponseFieldType.GetContentUnionType>uncheckedCast(
                                content));
            }

            @JsOverlay
            default void setContent(Uint8Array content) {
                setContent(
                        Js.<RemoteFileSourceClientMessage.ToObjectReturnType.MetaResponseFieldType.GetContentUnionType>uncheckedCast(
                                content));
            }

            @JsProperty
            void setError(String error);

            @JsProperty
            void setFound(boolean found);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SetExecutionContextFieldType {
            @JsOverlay
            static RemoteFileSourceClientMessage.ToObjectReturnType.SetExecutionContextFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<String> getResourcePathsList();

            @JsProperty
            void setResourcePathsList(JsArray<String> resourcePathsList);

            @JsOverlay
            default void setResourcePathsList(String[] resourcePathsList) {
                setResourcePathsList(Js.<JsArray<String>>uncheckedCast(resourcePathsList));
            }
        }

        @JsOverlay
        static RemoteFileSourceClientMessage.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        RemoteFileSourceClientMessage.ToObjectReturnType.MetaResponseFieldType getMetaResponse();

        @JsProperty
        String getRequestId();

        @JsProperty
        RemoteFileSourceClientMessage.ToObjectReturnType.SetExecutionContextFieldType getSetExecutionContext();

        @JsProperty
        void setMetaResponse(
                RemoteFileSourceClientMessage.ToObjectReturnType.MetaResponseFieldType metaResponse);

        @JsProperty
        void setRequestId(String requestId);

        @JsProperty
        void setSetExecutionContext(
                RemoteFileSourceClientMessage.ToObjectReturnType.SetExecutionContextFieldType setExecutionContext);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface MetaResponseFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetContentUnionType {
                @JsOverlay
                static RemoteFileSourceClientMessage.ToObjectReturnType0.MetaResponseFieldType.GetContentUnionType of(
                        Object o) {
                    return Js.cast(o);
                }

                @JsOverlay
                default String asString() {
                    return Js.asString(this);
                }

                @JsOverlay
                default Uint8Array asUint8Array() {
                    return Js.cast(this);
                }

                @JsOverlay
                default boolean isString() {
                    return (Object) this instanceof String;
                }

                @JsOverlay
                default boolean isUint8Array() {
                    return (Object) this instanceof Uint8Array;
                }
            }

            @JsOverlay
            static RemoteFileSourceClientMessage.ToObjectReturnType0.MetaResponseFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            RemoteFileSourceClientMessage.ToObjectReturnType0.MetaResponseFieldType.GetContentUnionType getContent();

            @JsProperty
            String getError();

            @JsProperty
            boolean isFound();

            @JsProperty
            void setContent(
                    RemoteFileSourceClientMessage.ToObjectReturnType0.MetaResponseFieldType.GetContentUnionType content);

            @JsOverlay
            default void setContent(String content) {
                setContent(
                        Js.<RemoteFileSourceClientMessage.ToObjectReturnType0.MetaResponseFieldType.GetContentUnionType>uncheckedCast(
                                content));
            }

            @JsOverlay
            default void setContent(Uint8Array content) {
                setContent(
                        Js.<RemoteFileSourceClientMessage.ToObjectReturnType0.MetaResponseFieldType.GetContentUnionType>uncheckedCast(
                                content));
            }

            @JsProperty
            void setError(String error);

            @JsProperty
            void setFound(boolean found);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SetExecutionContextFieldType {
            @JsOverlay
            static RemoteFileSourceClientMessage.ToObjectReturnType0.SetExecutionContextFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<String> getResourcePathsList();

            @JsProperty
            void setResourcePathsList(JsArray<String> resourcePathsList);

            @JsOverlay
            default void setResourcePathsList(String[] resourcePathsList) {
                setResourcePathsList(Js.<JsArray<String>>uncheckedCast(resourcePathsList));
            }
        }

        @JsOverlay
        static RemoteFileSourceClientMessage.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        RemoteFileSourceClientMessage.ToObjectReturnType0.MetaResponseFieldType getMetaResponse();

        @JsProperty
        String getRequestId();

        @JsProperty
        RemoteFileSourceClientMessage.ToObjectReturnType0.SetExecutionContextFieldType getSetExecutionContext();

        @JsProperty
        void setMetaResponse(
                RemoteFileSourceClientMessage.ToObjectReturnType0.MetaResponseFieldType metaResponse);

        @JsProperty
        void setRequestId(String requestId);

        @JsProperty
        void setSetExecutionContext(
                RemoteFileSourceClientMessage.ToObjectReturnType0.SetExecutionContextFieldType setExecutionContext);
    }

    public static native RemoteFileSourceClientMessage deserializeBinary(Uint8Array bytes);

    public static native RemoteFileSourceClientMessage deserializeBinaryFromReader(
            RemoteFileSourceClientMessage message, Object reader);

    public static native void serializeBinaryToWriter(
            RemoteFileSourceClientMessage message, Object writer);

    public static native RemoteFileSourceClientMessage.ToObjectReturnType toObject(
            boolean includeInstance, RemoteFileSourceClientMessage msg);

    public native void clearMetaResponse();

    public native void clearSetExecutionContext();

    public native RemoteFileSourceMetaResponse getMetaResponse();

    public native int getRequestCase();

    public native String getRequestId();

    public native SetExecutionContextRequest getSetExecutionContext();

    public native boolean hasMetaResponse();

    public native boolean hasSetExecutionContext();

    public native Uint8Array serializeBinary();

    public native void setMetaResponse();

    public native void setMetaResponse(RemoteFileSourceMetaResponse value);

    public native void setRequestId(String value);

    public native void setSetExecutionContext();

    public native void setSetExecutionContext(SetExecutionContextRequest value);

    public native RemoteFileSourceClientMessage.ToObjectReturnType0 toObject();

    public native RemoteFileSourceClientMessage.ToObjectReturnType0 toObject(boolean includeInstance);
}
