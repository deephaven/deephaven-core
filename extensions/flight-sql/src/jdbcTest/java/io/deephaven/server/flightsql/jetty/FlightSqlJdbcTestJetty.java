//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql.jetty;

import io.deephaven.server.flightsql.DaggerJettyTestComponent;
import io.deephaven.server.flightsql.FlightSqlJdbcTestBase;

public class FlightSqlJdbcTestJetty extends FlightSqlJdbcTestBase {

    @Override
    protected TestComponent component() {
        return DaggerJettyTestComponent.create();
    }
}
