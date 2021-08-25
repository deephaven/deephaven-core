package io.deephaven.javascript.proto.dhinternal.grpcweb.unary;

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
        name = "dhinternal.grpcWeb.unary.UnaryRpcOptions",
        namespace = JsPackage.GLOBAL)
public interface UnaryRpcOptions<TRequest, TResponse> extends RpcOptions {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetMetadataJsPropertyMapTypeParameterUnionType {
        @JsOverlay
        static UnaryRpcOptions.GetMetadataJsPropertyMapTypeParameterUnionType of(Object o) {
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
        static UnaryRpcOptions.GetMetadataUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default JsPropertyMap<UnaryRpcOptions.GetMetadataJsPropertyMapTypeParameterUnionType> asJsPropertyMap() {
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
    public interface OnEndFn<TResponse> {
        void onInvoke(UnaryOutput<TResponse> p0);
    }

    @JsOverlay
    static UnaryRpcOptions create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getHost();

    @JsProperty
    UnaryRpcOptions.GetMetadataUnionType getMetadata();

    @JsProperty
    UnaryRpcOptions.OnEndFn<TResponse> getOnEnd();

    @JsProperty
    TRequest getRequest();

    @JsProperty
    void setHost(String host);

    @JsOverlay
    default void setMetadata(BrowserHeaders metadata) {
        setMetadata(Js.<UnaryRpcOptions.GetMetadataUnionType>uncheckedCast(metadata));
    }

    @JsProperty
    void setMetadata(UnaryRpcOptions.GetMetadataUnionType metadata);

    @JsOverlay
    default void setMetadata(
            JsPropertyMap<UnaryRpcOptions.GetMetadataJsPropertyMapTypeParameterUnionType> metadata) {
        setMetadata(Js.<UnaryRpcOptions.GetMetadataUnionType>uncheckedCast(metadata));
    }

    @JsOverlay
    default void setMetadata(Object metadata) {
        setMetadata(Js.<UnaryRpcOptions.GetMetadataUnionType>uncheckedCast(metadata));
    }

    @JsOverlay
    default void setMetadata(String metadata) {
        setMetadata(Js.<UnaryRpcOptions.GetMetadataUnionType>uncheckedCast(metadata));
    }

    @JsProperty
    void setOnEnd(UnaryRpcOptions.OnEndFn<TResponse> onEnd);

    @JsProperty
    void setRequest(TRequest request);
}
