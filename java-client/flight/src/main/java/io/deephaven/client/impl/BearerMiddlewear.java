//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClientMiddleware;

import java.util.Objects;

class BearerMiddlewear implements FlightClientMiddleware {
    private final BearerHandler bearerHandler;

    BearerMiddlewear(BearerHandler bearerHandler) {
        this.bearerHandler = Objects.requireNonNull(bearerHandler);
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
        outgoingHeaders.insert(Authentication.AUTHORIZATION_HEADER.name(), bearerHandler.authenticationValue());
    }

    @Override
    public void onHeadersReceived(CallHeaders incomingHeaders) {
        String lastBearerValue = null;
        for (String authenticationValue : incomingHeaders.getAll(Authentication.AUTHORIZATION_HEADER.name())) {
            if (authenticationValue.startsWith(BearerHandler.BEARER_PREFIX)) {
                lastBearerValue = authenticationValue;
            }
        }
        if (lastBearerValue != null) {
            bearerHandler.setBearerToken(lastBearerValue.substring(BearerHandler.BEARER_PREFIX.length()));
        }
    }

    @Override
    public void onCallCompleted(CallStatus status) {

    }
}
