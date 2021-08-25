package io.deephaven.javascript.proto.dhinternal.grpcweb.service;

import io.deephaven.javascript.proto.dhinternal.grpcweb.message.ProtobufMessageClass;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.grpcWeb.service.MethodDefinition",
        namespace = JsPackage.GLOBAL)
public interface MethodDefinition<TRequest, TResponse> {
    @JsOverlay
    static MethodDefinition create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getMethodName();

    @JsProperty
    ProtobufMessageClass<TRequest> getRequestType();

    @JsProperty
    ProtobufMessageClass<TResponse> getResponseType();

    @JsProperty
    ServiceDefinition getService();

    @JsProperty
    boolean isRequestStream();

    @JsProperty
    boolean isResponseStream();

    @JsProperty
    void setMethodName(String methodName);

    @JsProperty
    void setRequestStream(boolean requestStream);

    @JsProperty
    void setRequestType(ProtobufMessageClass<TRequest> requestType);

    @JsProperty
    void setResponseStream(boolean responseStream);

    @JsProperty
    void setResponseType(ProtobufMessageClass<TResponse> responseType);

    @JsProperty
    void setService(ServiceDefinition service);
}
