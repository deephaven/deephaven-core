//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
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
        name = "dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceClientRequest",
        namespace = JsPackage.GLOBAL)
public class RemoteFileSourceClientRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface MetaResponseFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetContentUnionType {
                @JsOverlay
                static RemoteFileSourceClientRequest.ToObjectReturnType.MetaResponseFieldType.GetContentUnionType of(
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
            static RemoteFileSourceClientRequest.ToObjectReturnType.MetaResponseFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            RemoteFileSourceClientRequest.ToObjectReturnType.MetaResponseFieldType.GetContentUnionType getContent();

            @JsProperty
            String getError();

            @JsProperty
            boolean isFound();

            @JsProperty
            void setContent(
                    RemoteFileSourceClientRequest.ToObjectReturnType.MetaResponseFieldType.GetContentUnionType content);

            @JsOverlay
            default void setContent(String content) {
                setContent(
                        Js.<RemoteFileSourceClientRequest.ToObjectReturnType.MetaResponseFieldType.GetContentUnionType>uncheckedCast(
                                content));
            }

            @JsOverlay
            default void setContent(Uint8Array content) {
                setContent(
                        Js.<RemoteFileSourceClientRequest.ToObjectReturnType.MetaResponseFieldType.GetContentUnionType>uncheckedCast(
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
            static RemoteFileSourceClientRequest.ToObjectReturnType.SetExecutionContextFieldType create() {
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
        static RemoteFileSourceClientRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        RemoteFileSourceClientRequest.ToObjectReturnType.MetaResponseFieldType getMetaResponse();

        @JsProperty
        String getRequestId();

        @JsProperty
        RemoteFileSourceClientRequest.ToObjectReturnType.SetExecutionContextFieldType getSetExecutionContext();

        @JsProperty
        String getTestCommand();

        @JsProperty
        void setMetaResponse(
                RemoteFileSourceClientRequest.ToObjectReturnType.MetaResponseFieldType metaResponse);

        @JsProperty
        void setRequestId(String requestId);

        @JsProperty
        void setSetExecutionContext(
                RemoteFileSourceClientRequest.ToObjectReturnType.SetExecutionContextFieldType setExecutionContext);

        @JsProperty
        void setTestCommand(String testCommand);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface MetaResponseFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetContentUnionType {
                @JsOverlay
                static RemoteFileSourceClientRequest.ToObjectReturnType0.MetaResponseFieldType.GetContentUnionType of(
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
            static RemoteFileSourceClientRequest.ToObjectReturnType0.MetaResponseFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            RemoteFileSourceClientRequest.ToObjectReturnType0.MetaResponseFieldType.GetContentUnionType getContent();

            @JsProperty
            String getError();

            @JsProperty
            boolean isFound();

            @JsProperty
            void setContent(
                    RemoteFileSourceClientRequest.ToObjectReturnType0.MetaResponseFieldType.GetContentUnionType content);

            @JsOverlay
            default void setContent(String content) {
                setContent(
                        Js.<RemoteFileSourceClientRequest.ToObjectReturnType0.MetaResponseFieldType.GetContentUnionType>uncheckedCast(
                                content));
            }

            @JsOverlay
            default void setContent(Uint8Array content) {
                setContent(
                        Js.<RemoteFileSourceClientRequest.ToObjectReturnType0.MetaResponseFieldType.GetContentUnionType>uncheckedCast(
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
            static RemoteFileSourceClientRequest.ToObjectReturnType0.SetExecutionContextFieldType create() {
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
        static RemoteFileSourceClientRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        RemoteFileSourceClientRequest.ToObjectReturnType0.MetaResponseFieldType getMetaResponse();

        @JsProperty
        String getRequestId();

        @JsProperty
        RemoteFileSourceClientRequest.ToObjectReturnType0.SetExecutionContextFieldType getSetExecutionContext();

        @JsProperty
        String getTestCommand();

        @JsProperty
        void setMetaResponse(
                RemoteFileSourceClientRequest.ToObjectReturnType0.MetaResponseFieldType metaResponse);

        @JsProperty
        void setRequestId(String requestId);

        @JsProperty
        void setSetExecutionContext(
                RemoteFileSourceClientRequest.ToObjectReturnType0.SetExecutionContextFieldType setExecutionContext);

        @JsProperty
        void setTestCommand(String testCommand);
    }

    public static native RemoteFileSourceClientRequest deserializeBinary(Uint8Array bytes);

    public static native RemoteFileSourceClientRequest deserializeBinaryFromReader(
            RemoteFileSourceClientRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            RemoteFileSourceClientRequest message, Object writer);

    public static native RemoteFileSourceClientRequest.ToObjectReturnType toObject(
            boolean includeInstance, RemoteFileSourceClientRequest msg);

    public native void clearMetaResponse();

    public native void clearSetExecutionContext();

    public native void clearTestCommand();

    public native RemoteFileSourceMetaResponse getMetaResponse();

    public native int getRequestCase();

    public native String getRequestId();

    public native SetExecutionContextRequest getSetExecutionContext();

    public native String getTestCommand();

    public native boolean hasMetaResponse();

    public native boolean hasSetExecutionContext();

    public native boolean hasTestCommand();

    public native Uint8Array serializeBinary();

    public native void setMetaResponse();

    public native void setMetaResponse(RemoteFileSourceMetaResponse value);

    public native void setRequestId(String value);

    public native void setSetExecutionContext();

    public native void setSetExecutionContext(SetExecutionContextRequest value);

    public native void setTestCommand(String value);

    public native RemoteFileSourceClientRequest.ToObjectReturnType0 toObject();

    public native RemoteFileSourceClientRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
