package io.deephaven.client.impl;

import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.FlightClientMiddleware.Factory;

import java.util.Objects;

public class SessionMiddleware implements Factory {
    private final SessionImpl session;

    public SessionMiddleware(SessionImpl session) {
        this.session = Objects.requireNonNull(session);
    }

    @Override
    public final FlightClientMiddleware onCallStarted(CallInfo info) {
        return new AuthenticationMiddleware(session.auth());
    }
}
