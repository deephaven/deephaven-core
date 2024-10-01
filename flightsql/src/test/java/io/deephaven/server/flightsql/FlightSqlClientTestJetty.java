//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

public class FlightSqlClientTestJetty extends FlightSqlClientTestBase {

    @Override
    protected FlightSqlTestComponent component() {
        return DaggerJettyTestComponent.create();
    }
}
