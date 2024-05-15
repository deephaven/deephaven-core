//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.auth.AuthenticationException;
import io.deephaven.proto.DeephavenChannel;
import io.deephaven.proto.backplane.grpc.CloseSessionResponse;
import io.deephaven.proto.backplane.grpc.HandshakeRequest;
import io.deephaven.proto.backplane.grpc.HandshakeResponse;
import io.deephaven.server.session.SessionState;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.stub.MetadataUtils;
import org.junit.Before;

public abstract class DeephavenApiServerSingleAuthenticatedBase extends DeephavenApiServerTestBase {

    DeephavenChannel channel;

    String sessionToken;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        final DeephavenChannel channel = createChannel();
        final HandshakeResponse result =
                channel.sessionBlocking().newSession(HandshakeRequest.newBuilder().setAuthProtocol(1).build());
        // Note: the authentication token for DeephavenApiServerTestBase is valid for 7 days,
        // so we should only need to authenticate once :)
        sessionToken = result.getSessionToken().toStringUtf8();
        final String sessionHeader = result.getMetadataHeader().toStringUtf8();
        final Key<String> sessionHeaderKey = Metadata.Key.of(sessionHeader, Metadata.ASCII_STRING_MARSHALLER);
        final Metadata extraHeaders = new Metadata();
        extraHeaders.put(sessionHeaderKey, sessionToken);
        this.channel = DeephavenChannel.withClientInterceptors(channel,
                MetadataUtils.newAttachHeadersInterceptor(extraHeaders));
    }

    public SessionState authenticatedSessionState() {
        try {
            return server().sessionService().getSessionForAuthToken(sessionToken);
        } catch (AuthenticationException e) {
            throw new UncheckedDeephavenException();
        }
    }

    public DeephavenChannel channel() {
        return channel;
    }

    public CloseSessionResponse closeSession() {
        return channel.sessionBlocking().closeSession(HandshakeRequest.getDefaultInstance());
    }
}
