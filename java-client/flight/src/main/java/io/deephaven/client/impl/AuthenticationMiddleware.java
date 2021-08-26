package io.deephaven.client.impl;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClientMiddleware;

import java.util.Objects;

public class AuthenticationMiddleware implements FlightClientMiddleware {
    private final AuthenticationInfo auth;

    public AuthenticationMiddleware(AuthenticationInfo auth) {
        this.auth = Objects.requireNonNull(auth);
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
        outgoingHeaders.insert(auth.sessionHeaderKey(), auth.session());
    }

    @Override
    public void onHeadersReceived(CallHeaders incomingHeaders) {

    }

    @Override
    public void onCallCompleted(CallStatus status) {

    }
}
