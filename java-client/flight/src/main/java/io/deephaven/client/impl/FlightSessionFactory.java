package io.deephaven.client.impl;

import java.util.concurrent.CompletableFuture;

public interface FlightSessionFactory {
    FlightSession newFlightSession();

    CompletableFuture<? extends FlightSession> newFlightSessionFuture();
}
