//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import io.deephaven.server.flightsql.DeephavenServerTestBase.TestComponent;

public class FlightSqlClientTestJetty extends FlightSqlClientTestBase {

    @Override
    protected TestComponent component() {
        return DaggerJettyTestComponent.create();
    }
}
