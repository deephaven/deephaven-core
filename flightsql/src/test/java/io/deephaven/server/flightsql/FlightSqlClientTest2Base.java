//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import org.apache.arrow.flight.sql.FlightSqlClient;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public abstract class FlightSqlClientTest2Base extends DeephavenServerTestBase {


    @Override
    public void setup() throws IOException {
        super.setup();
    }

    @Override
    void tearDown() throws InterruptedException {
        super.tearDown();
    }
}
