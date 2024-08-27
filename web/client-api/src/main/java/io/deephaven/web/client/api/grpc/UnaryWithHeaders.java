//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.grpc;

import elemental2.promise.IThenable;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.grpcweb.Grpc;
import io.deephaven.javascript.proto.dhinternal.grpcweb.grpc.Code;
import io.deephaven.javascript.proto.dhinternal.grpcweb.unary.UnaryOutput;
import io.deephaven.javascript.proto.dhinternal.grpcweb.unary.UnaryRpcOptions;
import io.deephaven.web.client.api.WorkerConnection;

public class UnaryWithHeaders {

    /**
     * Improbable-eng's grpc-web implementation doesn't pass headers to api callers - this changes the contract a bit so
     * that we can get a typed UnaryOutput with the headers/trailers intact.
     * 
     * @param connection provides access to the metadata and the server url
     * @param methodDescriptor the service method to invoke
     * @return a promise that will resolve to the response plus headers/trailers, or reject with the headers/trailers
     * @param <Res> type of the message object
     */
    public static <Req, Res> Promise<UnaryOutput<Res>> call(WorkerConnection connection, Object methodDescriptor,
            Req request) {
        return new Promise<>((resolve, reject) -> {
            UnaryRpcOptions<Object, Object> props = UnaryRpcOptions.create();
            props.setHost(connection.configServiceClient().serviceHost);
            props.setMetadata(connection.metadata());
            props.setTransport(null);// ts doesn't expose these two, stick with defaults for now
            props.setDebug(false);
            props.setOnEnd(response -> {
                if (response.getStatus() != Code.OK) {
                    reject.onInvoke(response);
                } else {
                    resolve.onInvoke((IThenable<UnaryOutput<Res>>) response);
                }
            });
            props.setRequest(request);
            Grpc.unary.onInvoke(methodDescriptor, props);
        });
    }
}
