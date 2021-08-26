package io.deephaven.client.impl;

import io.deephaven.client.impl.FlightSession;

import java.util.concurrent.CompletableFuture;

public interface FlightSessionFactory {
    FlightSession newFlightSession();

    CompletableFuture<? extends FlightSession> newFlightSessionFuture();
}
