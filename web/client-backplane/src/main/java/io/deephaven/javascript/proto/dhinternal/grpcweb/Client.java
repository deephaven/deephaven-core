package io.deephaven.javascript.proto.dhinternal.grpcweb;

import io.deephaven.javascript.proto.dhinternal.grpcweb.client.ClientRpcOptions;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(isNative = true, name = "dhinternal.grpcWeb.client", namespace = JsPackage.GLOBAL)
public class Client {
    public static native <TRequest, TResponse, M> io.deephaven.javascript.proto.dhinternal.grpcweb.client.Client<TRequest, TResponse> client(
        M methodDescriptor, ClientRpcOptions props);
}
