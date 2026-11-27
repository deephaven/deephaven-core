//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.auth.AuthenticationException;
import io.deephaven.client.impl.Authentication;
import io.deephaven.client.impl.BearerHandler;
import io.deephaven.proto.DeephavenChannel;
import io.deephaven.proto.backplane.grpc.CloseSessionResponse;
import io.deephaven.proto.backplane.grpc.ConfigurationConstantsRequest;
import io.deephaven.proto.backplane.grpc.HandshakeRequest;
import io.deephaven.server.session.SessionState;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import org.junit.Before;

public abstract class DeephavenApiServerSingleAuthenticatedBase extends DeephavenApiServerTestBase {

    private static final String ANONYMOUS = "Anonymous";

    DeephavenChannel channel;

    BearerHandler bearerHandler;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        final DeephavenChannel channel = createChannel();
        bearerHandler = new BearerHandler();

        // As the Java client does, log in anonymously through GetConfigurationConstants; the bearer handler picks up
        // the session token from the response headers.
        final Metadata loginHeaders = new Metadata();
        loginHeaders.put(Authentication.AUTHORIZATION_HEADER, ANONYMOUS);
        DeephavenChannel.withClientInterceptors(channel,
                MetadataUtils.newAttachHeadersInterceptor(loginHeaders), bearerHandler)
                .configBlocking()
                .getConfigurationConstants(ConfigurationConstantsRequest.getDefaultInstance());

        // The bearer handler sends the current token on each call and follows any rotation in the responses.
        this.channel = DeephavenChannel.withClientInterceptors(
                DeephavenChannel.withCallCredentials(channel, bearerHandler), bearerHandler);
    }

    public SessionState authenticatedSessionState() {
        try {
            return server().sessionService()
                    .getSessionForAuthToken(BearerHandler.BEARER_PREFIX + bearerHandler.getCurrentToken());
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
