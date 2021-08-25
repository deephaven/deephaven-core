package io.deephaven.javascript.proto.dhinternal.grpcweb;

import io.deephaven.javascript.proto.dhinternal.grpcweb.invoke.InvokeRpcOptions;
import io.deephaven.javascript.proto.dhinternal.grpcweb.invoke.Request;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(isNative = true, name = "dhinternal.grpcWeb.invoke", namespace = JsPackage.GLOBAL)
public class Invoke {
    public static native <TRequest, TResponse, M> Request invoke(
            M methodDescriptor, InvokeRpcOptions<TRequest, TResponse> props);
}
