package io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport;

import elemental2.core.JsError;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.grpcweb.message.ProtobufMessage;
import io.deephaven.javascript.proto.dhinternal.grpcweb.service.MethodDefinition;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.grpcWeb.transports.Transport.TransportOptions",
    namespace = JsPackage.GLOBAL)
public interface TransportOptions {
    @JsFunction
    public interface OnChunkFn {
        void onInvoke(Uint8Array p0, boolean p1);
    }

    @JsFunction
    public interface OnEndFn {
        void onInvoke(JsError p0);
    }

    @JsFunction
    public interface OnHeadersFn {
        void onInvoke(BrowserHeaders p0, double p1);
    }

    @JsOverlay
    static TransportOptions create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    MethodDefinition<ProtobufMessage, ProtobufMessage> getMethodDefinition();

    @JsProperty
    TransportOptions.OnChunkFn getOnChunk();

    @JsProperty
    TransportOptions.OnEndFn getOnEnd();

    @JsProperty
    TransportOptions.OnHeadersFn getOnHeaders();

    @JsProperty
    String getUrl();

    @JsProperty
    boolean isDebug();

    @JsProperty
    void setDebug(boolean debug);

    @JsProperty
    void setMethodDefinition(MethodDefinition<ProtobufMessage, ProtobufMessage> methodDefinition);

    @JsProperty
    void setOnChunk(TransportOptions.OnChunkFn onChunk);

    @JsProperty
    void setOnEnd(TransportOptions.OnEndFn onEnd);

    @JsProperty
    void setOnHeaders(TransportOptions.OnHeadersFn onHeaders);

    @JsProperty
    void setUrl(String url);
}
