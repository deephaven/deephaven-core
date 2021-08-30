package io.deephaven.javascript.proto.dhinternal.grpcweb;

import io.deephaven.javascript.proto.dhinternal.grpcweb.grpc.Client;
import io.deephaven.javascript.proto.dhinternal.grpcweb.grpc.ClientRpcOptions;
import io.deephaven.javascript.proto.dhinternal.grpcweb.invoke.InvokeRpcOptions;
import io.deephaven.javascript.proto.dhinternal.grpcweb.invoke.Request;
import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.http.http.CrossBrowserHttpTransportInit;
import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.http.xhr.XhrTransportInit;
import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.TransportFactory;
import io.deephaven.javascript.proto.dhinternal.grpcweb.unary.UnaryRpcOptions;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(isNative = true, name = "dhinternal.grpcWeb.grpc", namespace = JsPackage.GLOBAL)
public class Grpc {
    @JsFunction
    public interface CrossBrowserHttpTransportFn {
        TransportFactory onInvoke(CrossBrowserHttpTransportInit p0);
    }

    @JsFunction
    public interface FetchReadableStreamTransportFn {
        TransportFactory onInvoke(Object p0);
    }

    @JsFunction
    public interface InvokeFn {
        Request onInvoke(Object p0, InvokeRpcOptions<Object, Object> p1);
    }

    @JsFunction
    public interface SetDefaultTransportFn {
        void onInvoke(TransportFactory p0);
    }

    @JsFunction
    public interface UnaryFn {
        Request onInvoke(Object p0, UnaryRpcOptions<Object, Object> p1);
    }

    @JsFunction
    public interface WebsocketTransportFn {
        TransportFactory onInvoke();
    }

    @JsFunction
    public interface XhrTransportFn {
        TransportFactory onInvoke(XhrTransportInit p0);
    }

    public static Grpc.CrossBrowserHttpTransportFn CrossBrowserHttpTransport;
    public static Grpc.FetchReadableStreamTransportFn FetchReadableStreamTransport;
    public static Grpc.WebsocketTransportFn WebsocketTransport;
    public static Grpc.XhrTransportFn XhrTransport;
    public static Grpc.InvokeFn invoke;
    public static Grpc.SetDefaultTransportFn setDefaultTransport;
    public static Grpc.UnaryFn unary;

    public static native <TRequest, TResponse, M> Client<TRequest, TResponse> client(
            M methodDescriptor, ClientRpcOptions props);

    public static native int httpStatusToCode(double httpStatus);
}
