//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import org.apache.arrow.flight.FlightClient;
import org.junit.jupiter.api.Test;

public abstract class FlightClientTestBase {

    public FlightClient flightClient() {
        return null;
    }

    @Test
    void listActions() throws InterruptedException {
        try (final FlightClient client = flightClient()) {
            client.listActions();
        }
    }
}
