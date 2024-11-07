//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.grpc;

import elemental2.promise.IThenable;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.grpcweb.Grpc;
import io.deephaven.javascript.proto.dhinternal.grpcweb.client.RpcOptions;
import io.deephaven.javascript.proto.dhinternal.grpcweb.grpc.Code;
import io.deephaven.javascript.proto.dhinternal.grpcweb.unary.UnaryOutput;
import io.deephaven.javascript.proto.dhinternal.grpcweb.unary.UnaryRpcOptions;

public class UnaryWithHeaders {

    /**
     * Improbable-eng's grpc-web implementation doesn't pass headers to api callers - this changes the contract a bit so
     * that we can get a typed UnaryOutput with the headers/trailers intact.
     *
     * @param host the server to connect to
     * @param metadata gRPC metadata to set on the request
     * @param options options to use when sending the request
     * @param methodDescriptor the service method to invoke
     * @return a promise that will resolve to the response plus headers/trailers, or reject with the headers/trailers
     * @param <Res> type of the message object
     */
    public static <Req, Res> Promise<UnaryOutput<Res>> call(String host, BrowserHeaders metadata, RpcOptions options,
            Object methodDescriptor,
            Req request) {
        return new Promise<>((resolve, reject) -> {
            UnaryRpcOptions<Object, Object> props = UnaryRpcOptions.create();
            props.setRequest(request);
            props.setHost(host);
            props.setMetadata(metadata);
            props.setTransport(options.getTransport());
            props.setDebug(options.isDebug());
            props.setOnEnd(response -> {
                if (response.getStatus() != Code.OK) {
                    reject.onInvoke(response);
                } else {
                    resolve.onInvoke((IThenable<UnaryOutput<Res>>) response);
                }
            });
            Grpc.unary.onInvoke(methodDescriptor, props);
        });
    }
}
