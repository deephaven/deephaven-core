//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner;

import dagger.Binds;
import dagger.multibindings.IntoSet;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This interceptor allows testing clients to hook into server-side RPC state.
 */
@Singleton
public final class RpcServerStateInterceptor implements ServerInterceptor {

    @dagger.Module
    interface Module {
        @Binds
        @IntoSet
        ServerInterceptor bindsInterceptor(RpcServerStateInterceptor interceptor);
    }

    private static final Key<String> KEY =
            Key.of(RpcServerStateInterceptor.class.getSimpleName(), Metadata.ASCII_STRING_MARSHALLER);

    private final Map<String, RpcServerState> map;

    @Inject
    public RpcServerStateInterceptor() {
        map = new ConcurrentHashMap<>();
    }

    /**
     * Creates a new {@link RpcServerState}.
     */
    public RpcServerState newRpcServerState() {
        final String id = UUID.randomUUID().toString();
        final RpcServerState rpcContext = new RpcServerState(id);
        map.put(id, rpcContext);
        return rpcContext;
    }

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {
        final String id = headers.get(KEY);
        if (id == null) {
            // No RpcServerState requested, bypass.
            return next.startCall(call, headers);
        }
        final RpcServerState state = map.remove(id);
        if (state == null) {
            throw new IllegalStateException(String.format(
                    "Re-use error for id='%s'. The test is probably re-using RpcServerState#clientInterceptor for multiple RPCs which is not allowed.",
                    id));
        }
        return state.intercept(call, headers, next);
    }

    public static final class RpcServerState {
        private final CountDownLatch startCall;
        private final CountDownLatch onHalfClosed;
        private final AtomicReference<ClientInterceptor> clientInterceptor;

        private MethodDescriptor<?, ?> methodDescriptor;

        RpcServerState(String id) {
            this.startCall = new CountDownLatch(1);
            this.onHalfClosed = new CountDownLatch(1);
            final Metadata metadata = new Metadata();
            metadata.put(KEY, id);
            this.clientInterceptor = new AtomicReference<>(MetadataUtils.newAttachHeadersInterceptor(metadata));
        }

        /**
         * The necessary, additional logic to pass along {@code this} state to the server. Callers should use this
         * method exactly once in combination with a single RPC via
         * {@link AbstractStub#withInterceptors(ClientInterceptor...)}.
         */
        public ClientInterceptor clientInterceptor() {
            final ClientInterceptor clientInterceptor = this.clientInterceptor.getAndSet(null);
            if (clientInterceptor == null) {
                throw new IllegalStateException("Tests should call clientInterceptor at most once");
            }
            return clientInterceptor;
        }

        /**
         * Waits for the initial server-side invoke to finish.
         */
        public void awaitServerInvokeFinished(Duration timeout) throws InterruptedException, TimeoutException {
            if (clientInterceptor.get() != null) {
                throw new IllegalStateException("Tests should call clientInterceptor() before waiting");
            }
            if (!startCall.await(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
                throw new TimeoutException();
            }
            // We could be more a bit more efficient here and have the testing client pass in the MethodDescriptor, but
            // that would increase the complexity for the testing client.
            if (methodDescriptor.getType().clientSendsOneMessage()) {
                // In the case where we know the client only sends one message, we're going to wait for the server to
                // finish the client half-close handling. This matches the GRPC implementation in
                // io.grpc.stub.ServerCalls.UnaryServerCallHandler.UnaryServerCallListener. Even if the GRPC impl
                // becomes more aggressive in the future (ie, actually invokes the server during the onMessage), we can
                // still be safe here with this more conservative approach.
                if (!onHalfClosed.await(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
                    throw new TimeoutException();
                }
            } else {
                // In the case where the client is streaming (either one way client streaming or bidir), the server gets
                // invoked during the startCall; in which case, we've already waited for the startCall and the server
                // implementation has already been invoked.
            }
        }

        <RespT, ReqT> Listener<ReqT> intercept(ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {
            final Context context = Context.current();
            final Listener<ReqT> listener = Contexts.interceptCall(context, call, headers, next);
            this.methodDescriptor = call.getMethodDescriptor();
            // We may find unit-testing use-cases where we'd like to make further information available to the testing
            // client, in which case we might end up saving call, headers, context, or listener.
            // this.call = call;
            // this.headers = headers;
            // this.context = context;
            // this.listener = listener;
            // startCall happens in interceptCall
            startCall.countDown();
            return new SimpleForwardingServerCallListener<>(listener) {

                @Override
                public void onHalfClose() {
                    super.onHalfClose();
                    onHalfClosed.countDown();
                }
            };
        }
    }
}
