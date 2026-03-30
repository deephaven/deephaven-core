//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.stream;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

/**
 * Captures the http trailers for a given call and makes them available in the Context.
 */
public class TrailersCapturingInterceptor implements ClientInterceptor {

    private static final Context.Key<Metadata> TRAILERS_KEY = Context.key("trailers");

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(responseListener) {
                    @Override
                    public void onClose(Status status, Metadata trailers) {
                        // Store the trailers in the Context
                        Context.current().withValue(TRAILERS_KEY, trailers).run(() -> {
                            super.onClose(status, trailers);
                        });
                    }
                }, headers);
            }
        };
    }

    public static Metadata getTrailersFromContext() {
        return TRAILERS_KEY.get();
    }
}
