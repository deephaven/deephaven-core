package io.deephaven.server.runner;

import io.deephaven.grpc_api.DeephavenChannel;
import io.deephaven.proto.backplane.grpc.HandshakeRequest;
import io.deephaven.proto.backplane.grpc.HandshakeResponse;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.MethodDescriptor;
import org.junit.Before;

import java.util.UUID;

public abstract class DeephavenApiServerSingleAuthenticatedBase extends DeephavenApiServerTestBase {

    DeephavenChannel channel;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        final DeephavenChannel channel = channel();
        final HandshakeResponse result =
                channel.sessionBlocking().newSession(HandshakeRequest.newBuilder().setAuthProtocol(1).build());
        // Note: the authentication token for DeephavenApiServerTestBase is valid for 7 days,
        // so we should only need to authenticate once :)
        final UUID session = UUID.fromString(result.getSessionToken().toStringUtf8());
        final String sessionHeader = result.getMetadataHeader().toStringUtf8();
        final Key<String> sessionHeaderKey = Metadata.Key.of(sessionHeader, Metadata.ASCII_STRING_MARSHALLER);
        final Channel authenticatedChannel = ClientInterceptors.intercept(channel.channel(), new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                    final MethodDescriptor<ReqT, RespT> methodDescriptor, final CallOptions callOptions,
                    final Channel channel) {
                return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                        channel.newCall(methodDescriptor, callOptions)) {
                    @Override
                    public void start(final Listener<RespT> responseListener, final Metadata headers) {
                        headers.put(sessionHeaderKey, session.toString());
                        super.start(responseListener, headers);
                    }
                };
            }
        });
        this.channel = new DeephavenChannel(authenticatedChannel);
    }
}
