package io.deephaven.javascript.proto.dhinternal.grpcweb.invoke;

import elemental2.core.JsArray;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.grpcweb.client.RpcOptions;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.grpcWeb.invoke.InvokeRpcOptions",
        namespace = JsPackage.GLOBAL)
public interface InvokeRpcOptions<TRequest, TResponse> extends RpcOptions {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetMetadataJsPropertyMapTypeParameterUnionType {
        @JsOverlay
        static InvokeRpcOptions.GetMetadataJsPropertyMapTypeParameterUnionType of(Object o) {
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
    public interface GetMetadataUnionType {
        @JsOverlay
        static InvokeRpcOptions.GetMetadataUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default JsPropertyMap<InvokeRpcOptions.GetMetadataJsPropertyMapTypeParameterUnionType> asJsPropertyMap() {
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

    @JsFunction
    public interface OnEndFn {
        void onInvoke(int p0, String p1, BrowserHeaders p2);
    }

    @JsFunction
    public interface OnHeadersFn {
        void onInvoke(BrowserHeaders p0);
    }

    @JsFunction
    public interface OnMessageFn<TResponse> {
        void onInvoke(TResponse p0);
    }

    @JsOverlay
    static InvokeRpcOptions create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getHost();

    @JsProperty
    InvokeRpcOptions.GetMetadataUnionType getMetadata();

    @JsProperty
    InvokeRpcOptions.OnEndFn getOnEnd();

    @JsProperty
    InvokeRpcOptions.OnHeadersFn getOnHeaders();

    @JsProperty
    InvokeRpcOptions.OnMessageFn<TResponse> getOnMessage();

    @JsProperty
    TRequest getRequest();

    @JsProperty
    void setHost(String host);

    @JsOverlay
    default void setMetadata(BrowserHeaders metadata) {
        setMetadata(Js.<InvokeRpcOptions.GetMetadataUnionType>uncheckedCast(metadata));
    }

    @JsProperty
    void setMetadata(InvokeRpcOptions.GetMetadataUnionType metadata);

    @JsOverlay
    default void setMetadata(
            JsPropertyMap<InvokeRpcOptions.GetMetadataJsPropertyMapTypeParameterUnionType> metadata) {
        setMetadata(Js.<InvokeRpcOptions.GetMetadataUnionType>uncheckedCast(metadata));
    }

    @JsOverlay
    default void setMetadata(Object metadata) {
        setMetadata(Js.<InvokeRpcOptions.GetMetadataUnionType>uncheckedCast(metadata));
    }

    @JsOverlay
    default void setMetadata(String metadata) {
        setMetadata(Js.<InvokeRpcOptions.GetMetadataUnionType>uncheckedCast(metadata));
    }

    @JsProperty
    void setOnEnd(InvokeRpcOptions.OnEndFn onEnd);

    @JsProperty
    void setOnHeaders(InvokeRpcOptions.OnHeadersFn onHeaders);

    @JsProperty
    void setOnMessage(InvokeRpcOptions.OnMessageFn<? super TResponse> onMessage);

    @JsProperty
    void setRequest(TRequest request);
}
