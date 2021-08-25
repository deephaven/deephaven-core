package io.deephaven.javascript.proto.dhinternal.grpcweb.client;

import elemental2.core.JsArray;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(isNative = true, name = "dhinternal.grpcWeb.client.Client", namespace = JsPackage.GLOBAL)
public interface Client<TRequest, TResponse> {
    @JsFunction
    public interface OnEndCallbackFn {
        void onInvoke(int p0, String p1, BrowserHeaders p2);
    }

    @JsFunction
    public interface OnHeadersCallbackFn {
        void onInvoke(BrowserHeaders p0);
    }

    @JsFunction
    public interface OnMessageCallbackFn<TResponse> {
        void onInvoke(TResponse p0);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface StartMetadataJsPropertyMapTypeParameterUnionType {
        @JsOverlay
        static Client.StartMetadataJsPropertyMapTypeParameterUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default JsArray<String> asJsArray() {
            return Js.cast(this);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default boolean isJsArray() {
            return (Object) this instanceof JsArray;
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface StartMetadataUnionType {
        @JsOverlay
        static Client.StartMetadataUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default JsPropertyMap<Client.StartMetadataJsPropertyMapTypeParameterUnionType> asJsPropertyMap() {
            return Js.cast(this);
        }

        @JsOverlay
        default Object asObject() {
            return Js.cast(this);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isObject() {
            return (Object) this instanceof Object;
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }
    }

    void close();

    void finishSend();

    void onEnd(Client.OnEndCallbackFn callback);

    void onHeaders(Client.OnHeadersCallbackFn callback);

    void onMessage(Client.OnMessageCallbackFn<? super TResponse> callback);

    void send(TRequest message);

    void start();

    @JsOverlay
    default void start(BrowserHeaders metadata) {
        start(Js.<Client.StartMetadataUnionType>uncheckedCast(metadata));
    }

    @JsOverlay
    default void start(
        JsPropertyMap<Client.StartMetadataJsPropertyMapTypeParameterUnionType> metadata) {
        start(Js.<Client.StartMetadataUnionType>uncheckedCast(metadata));
    }

    @JsOverlay
    default void start(Object metadata) {
        start(Js.<Client.StartMetadataUnionType>uncheckedCast(metadata));
    }

    void start(Client.StartMetadataUnionType metadata);

    @JsOverlay
    default void start(String metadata) {
        start(Js.<Client.StartMetadataUnionType>uncheckedCast(metadata));
    }
}
